import express from 'express';
import cors from 'cors';
import fetch from 'node-fetch';
import protobuf from 'protobufjs';
import ScheduleLoader from './schedule-loader.js';

// In server.js, change the imports to:
import {
  extractBlockIdFromTripId,
  getShapeIdFromTrip,
  isTripCurrentlyActive,
  findCurrentSegmentAndProgress,  // Changed name
  calculateVirtualBusPosition,    // New function
  calculateVirtualBusPositionForBlock, // NEW: Block-based calculation
  getRouteDisplayName,
  timeStringToSeconds,            
  getScheduleTimeInSeconds,
  isTripActiveInStaticSchedule,
  getStaticScheduleForTrip,
  isTripScheduledToday,           // NEW: Add this import
  isTripActiveAndScheduled,       // NEW: Add this import
  getScheduleTimeFromUnix,
  isBlockActive,                  // NEW
  findCurrentOrRecentTripInBlock, // NEW
  enrichWithHeadsign
} from './virtual-vehicles.js';

//construct Cache to throttle back virtual bus feed to 5 seconds
const virtualBusesCache = new Map(); // operatorId -> { virtualEntities, timestamp }
const virtualsCache = new Map(); // operatorId -> { data, timestamp }

const scheduleLoader = new ScheduleLoader();
const app = express();
app.use(cors());

// Constants
const GTFS_PROTO_URL = 'https://raw.githubusercontent.com/google/transit/master/gtfs-realtime/proto/gtfs-realtime.proto';
const BASE_URL = 'https://bct.tmix.se/gtfs-realtime';
const DEFAULT_OPERATOR_ID = '36';
let root = null;

// Helper to get unique block IDs from trips
function getBlockIdsFromTripIds(tripIds, scheduleData) {
  const blockIds = new Set();
  
  tripIds.forEach(tripId => {
    const blockId = extractBlockIdFromTripId(tripId);
    if (blockId) {
      // Also check if block exists in schedule
      let blockExists = false;
      for (const trip of Object.values(scheduleData.tripsMap || {})) {
        if (trip.block_id === blockId) {
          blockExists = true;
          break;
        }
      }
      if (blockExists) {
        blockIds.add(blockId);
      }
    }
  });
  
  return Array.from(blockIds);
}

// Helper to get all trips for a block
function getTripIdsForBlock(blockId, scheduleData) {
  const tripIds = [];
  
  for (const [tripId, trip] of Object.entries(scheduleData.tripsMap || {})) {
    if (trip.block_id === blockId) {
      tripIds.push(tripId);
    }
  }
  
  return tripIds;
}

// Helper to get route ID for a block (use first trip's route)
function getRouteIdForBlock(blockId, scheduleData) {
  for (const trip of Object.values(scheduleData.tripsMap || {})) {
    if (trip.block_id === blockId) {
      return trip.route_id;
    }
  }
  return 'UNKNOWN';
}

//clean caches periodically
setInterval(() => {
  const now = Math.floor(Date.now() / 1000);
  let cleanedVirtuals = 0;
  let cleanedBuses = 0;
  
  // Clean virtualsCache
  for (const [key, value] of virtualsCache.entries()) {
    if (now - value.timestamp > 300) {
      virtualsCache.delete(key);
      cleanedVirtuals++;
    }
  }
  
  // Clean virtualBusesCache
  for (const [key, value] of virtualBusesCache.entries()) {
    if (now - value.timestamp > 300) {
      virtualBusesCache.delete(key);
      cleanedBuses++;
    }
  }
  
  if (cleanedVirtuals > 0 || cleanedBuses > 0) {
    console.log(`[CACHE-CLEANUP] Removed ${cleanedVirtuals} from virtualsCache, ${cleanedBuses} from virtualBusesCache`);
    console.log(`[CACHE-STATS] Current: virtualsCache=${virtualsCache.size}, virtualBusesCache=${virtualBusesCache.size}`);
  }
}, 60000); // Run every minute

// helper function
function formatTime(seconds) {
  const hrs = Math.floor(seconds / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  const secs = seconds % 60;
  return `${hrs.toString().padStart(2, '0')}:${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
}

// Load proto once
async function loadProto() {
  try {
    const response = await fetch(GTFS_PROTO_URL);
    const protoText = await response.text();
    root = protobuf.parse(protoText).root;
    console.log('✅ GTFS proto loaded');
  } catch (error) {
    console.error('❌ Failed to load proto:', error);
  }
}

// Fetch + decode GTFS feed
async function fetchGTFSFeed(feedType, operatorId = DEFAULT_OPERATOR_ID) {
  if (!root) throw new Error('Proto not loaded');
  const url = `${BASE_URL}/${feedType}?operatorIds=${operatorId}`;
  try {
    const response = await fetch(url);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const buffer = await response.arrayBuffer();
    const FeedMessage = root.lookupType('transit_realtime.FeedMessage');
    const message = FeedMessage.decode(new Uint8Array(buffer));
    const data = FeedMessage.toObject(message, { defaults: true, longs: String, enums: String, bytes: String });
    return { success: true, data, url, timestamp: new Date().toISOString() };
  } catch (error) {
    return { success: false, error: error.message, url, timestamp: new Date().toISOString() };
  }
}

// Add parsed blockId to vehicle entities
function addParsedBlockIdToVehicles(vehicleEntities) {
  if (!Array.isArray(vehicleEntities)) return vehicleEntities || [];
  return vehicleEntities.map(entity => {
    const processed = JSON.parse(JSON.stringify(entity));
    const tripId = processed.vehicle?.trip?.tripId;
    if (tripId) {
      const blockId = extractBlockIdFromTripId(tripId);
      if (blockId) {
        if (!processed.vehicle.trip) processed.vehicle.trip = {};
        processed.vehicle.trip.blockId = blockId;
      }
    }
    return processed;
  });
}

// Add parsed blockId to trip updates
function addParsedBlockIdToTripUpdates(tripUpdateEntities) {
  if (!Array.isArray(tripUpdateEntities)) return tripUpdateEntities || [];
  return tripUpdateEntities.map(entity => {
    const processed = JSON.parse(JSON.stringify(entity));
    const tripId = processed.tripUpdate?.trip?.tripId;
    if (tripId) {
      const blockId = extractBlockIdFromTripId(tripId);
      if (blockId) {
        if (!processed.tripUpdate.trip) processed.tripUpdate.trip = {};
        processed.tripUpdate.trip.blockId = blockId;
      }
    }
    return processed;
  });
}

// Ensure schedule loaded
async function ensureScheduleLoaded() {
  if (!scheduleLoader.scheduleData?.tripsMap || Object.keys(scheduleLoader.scheduleData.tripsMap).length === 0) {
    console.log('[ensureSchedule] Reloading...');
    await scheduleLoader.loadSchedules();
  }
}

// Main virtuals endpoint with 5-second throttling
// Main virtuals endpoint with 5-second throttling
app.get('/api/virtuals', async (req, res) => {
  try {
    const operatorId = req.query.operatorId || DEFAULT_OPERATOR_ID;
    const allVirtuals = req.query.all_virtuals === 'true';
    const forceRefresh = req.query.force === 'true';
    const currentTimeSec = Math.floor(Date.now() / 1000);
    const start = Date.now();

    const formatTime = (seconds) => {
      const hrs = Math.floor(seconds / 3600);
      const mins = Math.floor((seconds % 3600) / 60);
      const secs = seconds % 60;
      return `${hrs.toString().padStart(2, '0')}:${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
    };

    const cacheKey = `${operatorId}-${allVirtuals}`;
    const cached = virtualsCache.get(cacheKey);

    if (!forceRefresh && cached && (currentTimeSec - cached.timestamp < 5)) {
      console.log(`[VIRTUALS] Returning cached response (${currentTimeSec - cached.timestamp}s old)`);
      const response = JSON.parse(JSON.stringify(cached.data));
      response.header.timestamp = currentTimeSec;
      response.metadata.cached = true;
      response.metadata.cachedForSeconds = currentTimeSec - cached.timestamp;
      response.metadata.generatedAt = cached.timestamp;
      response.metadata.responseTimeMs = Date.now() - start;
      console.log(`[VIRTUALS] Done: ${response.entity.length} virtual buses from cache`);
      return res.json(response);
    }

    console.log(`[VIRTUALS] Called | op=${operatorId} | all=${allVirtuals} | force=${forceRefresh} | currentTime=${currentTimeSec}`);

    const [vehicleResult, tripResult] = await Promise.all([
      fetchGTFSFeed('vehicleupdates.pb', operatorId),
      fetchGTFSFeed('tripupdates.pb', operatorId)
    ]);

    if (!tripResult.success) {
      return res.status(503).json({ error: 'Trip feed unavailable' });
    }

    await ensureScheduleLoaded();
    const schedule = scheduleLoader.scheduleData;

    if (!schedule?.tripsMap || !schedule?.shapes || !schedule?.stopTimesByTrip) {
      console.error('[VIRTUALS] Schedule incomplete');
      return res.status(500).json({ error: 'Schedule incomplete' });
    }

    const realVehicleTripIds = new Set();
    if (vehicleResult.success && vehicleResult.data?.entity) {
      vehicleResult.data.entity.forEach(entity => {
        const tripId = entity.vehicle?.trip?.tripId;
        if (tripId) realVehicleTripIds.add(tripId);
      });
    }

    const virtualEntities = [];
    const processedBlocks = new Set();

    const blockIdsFromFeed = new Set();
    tripResult.data.entity?.forEach(entity => {
      const tu = entity.tripUpdate;
      if (tu?.trip?.tripId) {
        const blockId = extractBlockIdFromTripId(tu.trip.tripId);
        if (blockId) blockIdsFromFeed.add(blockId);
      }
    });

    const allBlockIds = getBlockIdsFromTripIds(Object.keys(schedule.stopTimesByTrip || {}), schedule);
    const allBlocks = new Set([...blockIdsFromFeed, ...allBlockIds]);

    console.log(`[VIRTUALS] Processing ${allBlocks.size} unique blocks`);

    allBlocks.forEach(blockId => {
      if (processedBlocks.has(blockId)) return;
      processedBlocks.add(blockId);

      const tripIdsInBlock = getTripIdsForBlock(blockId, schedule);
      if (tripIdsInBlock.length === 0) return;

      let hasScheduledTripToday = false;
      let firstScheduledTripId = null;

      for (const tripId of tripIdsInBlock) {
        if (isTripScheduledToday(tripId, schedule)) {
          hasScheduledTripToday = true;
          firstScheduledTripId = tripId;
          break;
        }
      }

      if (!hasScheduledTripToday) return;

      const currentScheduleSec = getScheduleTimeInSeconds(operatorId);
      if (!isBlockActive(blockId, schedule, currentScheduleSec, operatorId)) return;

      const position = calculateVirtualBusPositionForBlock(blockId, schedule, currentTimeSec, operatorId);
      if (!position) return;

      const hasRealVehicle = tripIdsInBlock.some(tripId => realVehicleTripIds.has(tripId));

      if (!allVirtuals && hasRealVehicle) {
        console.log(`[VIRTUALS] Skipping block ${blockId} - real vehicle exists`);
        return;
      }

      const vehicleId = `VIRT-${blockId}`;

      const headsign = position.trip?.tripHeadsign || '';
      const routeDisplay = getRouteDisplayName(position.trip?.routeId || 'UNKNOWN');
      const label = headsign ? `${routeDisplay} - ${headsign}` : `Ghost ${routeDisplay}`;

      const virtualEntity = {
        id: vehicleId,
        vehicle: {
          trip: {
            tripId: position.trip?.tripId || firstScheduledTripId,
            startTime: position.trip?.startTime || '—',
            startDate: position.trip?.startDate || getCurrentDateStr(),
            routeId: position.trip?.routeId || 'UNKNOWN',
            directionId: position.trip?.directionId ?? 0,
            blockId: blockId,
            tripHeadsign: headsign || null
          },
          position: {
            latitude: position.latitude,
            longitude: position.longitude,
            bearing: position.bearing ?? null,
            speed: position.speed ?? 0
          },
          currentStopSequence: position.currentStopSequence ?? 1,
          currentStatus: position.currentStatus ?? (position.layover ? 1 : 2),
          timestamp: position.timestamp ?? currentTimeSec,
          stopId: position.stopId ?? (position.segment?.start || position.segment?.end || null),
          vehicle: {
            id: vehicleId,
            label: label,
            is_virtual: true
          },
          progress: Number.isFinite(position.progress) 
  ? Number(position.progress).toFixed(3) 
  : '0.000',
          metadata: {
            source: position.metadata?.source || 'block_schedule',
            all_virtuals_mode: allVirtuals,
            has_real_vehicle: hasRealVehicle,
            crossed_midnight: position.crossedMidnight || false,
            layover: position.layover || false,
            status: position.status || 'UNKNOWN',
            block_trips_count: tripIdsInBlock.length
          }
        }
      };

      virtualEntities.push(virtualEntity);
      console.log(`[VIRTUALS] Added virtual bus for block ${blockId} at ${position.latitude},${position.longitude} (headsign: ${headsign || 'none'})`);
    });

    const took = Date.now() - start;

    const response = {
      header: {
        gtfs_realtime_version: "2.0",
        incrementality: "FULL_DATASET",
        timestamp: currentTimeSec
      },
      entity: virtualEntities,
      metadata: {
        operatorId,
        fetchedAt: new Date().toISOString(),
        responseTimeMs: took,
        mode: allVirtuals ? 'all_virtuals' : 'missing_only',
        generated: virtualEntities.length,
        totalTripsInFeed: tripResult.data.entity?.length || 0,
        processedBlocks: processedBlocks.size,
        realVehiclesCount: realVehicleTripIds.size,
        // ... (keep your existing metadata fields)
      }
    };

    virtualsCache.set(cacheKey, { data: response, timestamp: currentTimeSec });

    console.log(`[VIRTUALS] Done: ${virtualEntities.length} virtual buses in ${took}ms`);
    res.json(response);

  } catch (err) {
    console.error('[VIRTUALS] Error:', err);
    res.status(500).json({ error: 'Failed to generate virtual buses', details: err.message });
  }
});

// Keep /api/buses (real + virtual combined) — with caching
app.get('/api/buses', async (req, res) => {
  if (!root) return res.status(500).json({ error: 'Proto not loaded' });
  try {
    const operatorId = req.query.operatorId || DEFAULT_OPERATOR_ID;
    const noVirtuals = 'no_virtuals' in req.query;
    const allVirtuals = req.query.all_virtuals === 'true';
    const forceRefresh = req.query.force === 'true';
    const startTime = Date.now();
    const currentTimeSec = Math.floor(Date.now() / 1000);
    const currentScheduleSec = getScheduleTimeInSeconds(operatorId);
    console.log(`[${new Date().toISOString()}] /api/buses called | operator=${operatorId} | no_virtuals=${noVirtuals} | all_virtuals=${allVirtuals}`);

    // ALWAYS fetch fresh real feeds first
    const [vehicleResult, tripResult, alertsResult] = await Promise.all([
      fetchGTFSFeed('vehicleupdates.pb', operatorId),
      fetchGTFSFeed('tripupdates.pb', operatorId),
      fetchGTFSFeed('alerts.pb', operatorId)
    ]);

    // Load schedule EARLY so it's available for real + virtual processing
    await ensureScheduleLoaded();
    const schedule = scheduleLoader.scheduleData;

    if (!schedule?.tripsMap) {
      console.warn('[/api/buses] Schedule loaded but tripsMap is missing or empty');
    }

    // Process real vehicle positions (add blockId + headsign enrichment)
    let processedVehicles = [];
    if (vehicleResult.success && vehicleResult.data?.entity) {
      // First add parsed blockId
      processedVehicles = addParsedBlockIdToVehicles(vehicleResult.data.entity);

      // Then enrich with trip_headsign from static schedule (trips.txt)
      if (schedule?.tripsMap) {
        processedVehicles = enrichWithHeadsign(processedVehicles, schedule);
      } else {
        console.warn('[/api/buses] Skipping headsign enrichment — tripsMap not available');
      }
    }

    // Process real trip updates (add blockId parsing)
    let processedTrips = tripResult.success ? addParsedBlockIdToTripUpdates(tripResult.data.entity || []) : [];

    // VIRTUAL BUSES: Check cache (5-second throttle)
    let virtualEntities = [];
    if (!noVirtuals) {
      const cacheKey = `buses-${operatorId}-${allVirtuals}`;
      const cached = virtualBusesCache.get(cacheKey);
      if (!forceRefresh && cached && (Date.now() - cached.timestamp < 5000)) {
        console.log(`[/api/buses] Using cached virtual buses (${Date.now() - cached.timestamp}ms old)`);
        virtualEntities = cached.virtualEntities;
      } else {
        // Generate fresh virtual buses
        if (!schedule?.tripsMap || !schedule?.stopTimesByTrip) {
          console.log('[/api/buses] Schedule incomplete, skipping virtual buses');
        } else {
          // Get real vehicle trips for conflict check
          const realVehicleTripIds = new Set();
          if (vehicleResult.success && vehicleResult.data?.entity) {
            vehicleResult.data.entity.forEach(entity => {
              const tripId = entity.vehicle?.trip?.tripId;
              if (tripId) realVehicleTripIds.add(tripId);
            });
          }

          virtualEntities = [];
          const processedVirtualBlocks = new Set();
          const allBlocks = getBlockIdsFromTripIds(Object.keys(schedule.stopTimesByTrip || {}), schedule);

          allBlocks.forEach(blockId => {
            if (processedVirtualBlocks.has(blockId)) return;
            processedVirtualBlocks.add(blockId);

            const tripIdsInBlock = getTripIdsForBlock(blockId, schedule);
            if (tripIdsInBlock.length === 0) return;

            // Must have at least one trip scheduled today
            let hasScheduledTripToday = false;
            for (const tripId of tripIdsInBlock) {
              if (isTripScheduledToday(tripId, schedule)) {
                hasScheduledTripToday = true;
                break;
              }
            }
            if (!hasScheduledTripToday) return;

            // Block must be active (with 10-min buffer before first trip)
            if (!isBlockActive(blockId, schedule, currentScheduleSec, operatorId)) return;

            // Only skip virtual if NOT in all_virtuals mode AND real vehicle exists
            const hasRealVehicle = tripIdsInBlock.some(tripId => realVehicleTripIds.has(tripId));
            if (!allVirtuals && hasRealVehicle) {
              console.log(`[/api/buses] Skipping block ${blockId} - real vehicle exists`);
              return;
            }

            const position = calculateVirtualBusPositionForBlock(blockId, schedule, currentTimeSec, operatorId);
            if (!position) return;

            const vehicleId = `VIRT-${blockId}`;
            const headsign = position.trip?.tripHeadsign || '';
            const routeDisplay = getRouteDisplayName(position.trip?.routeId || 'UNKNOWN');
            const label = headsign ? `${routeDisplay} - ${headsign}` : `Ghost ${routeDisplay}`;

            virtualEntities.push({
              id: vehicleId,
              vehicle: {
                trip: {
                  tripId: position.trip?.tripId || tripIdsInBlock[0],
                  startTime: position.trip?.startTime || '—',
                  startDate: position.trip?.startDate || getCurrentDateStr(),
                  routeId: position.trip?.routeId || 'UNKNOWN',
                  directionId: position.trip?.directionId ?? 0,
                  blockId: blockId,
                  tripHeadsign: headsign || null
                },
                position: {
                  latitude: position.latitude,
                  longitude: position.longitude,
                  bearing: position.bearing ?? null,
                  speed: position.speed ?? 0
                },
                currentStopSequence: position.currentStopSequence ?? 1,
                currentStatus: position.currentStatus ?? (position.layover ? 1 : 2),
                timestamp: position.timestamp ?? currentTimeSec,
                stopId: position.stopId ?? (position.segment?.start || position.segment?.end || null),
                vehicle: {
                  id: vehicleId,
                  label: label,
                  is_virtual: true
                },
                progress: Number.isFinite(position.progress)
                  ? Number(position.progress).toFixed(3)
                  : '0.000',
                metadata: {
                  source: position.metadata?.source || 'block_schedule',
                  all_virtuals_mode: allVirtuals,
                  has_real_vehicle: hasRealVehicle,
                  crossed_midnight: position.crossedMidnight || false,
                  layover: position.layover || false,
                  status: position.status || 'UNKNOWN',
                  block_trips_count: tripIdsInBlock.length
                }
              }
            });

            console.log(`[/api/buses] Added virtual bus for block ${blockId} (${routeDisplay}) at ${position.latitude},${position.longitude}`);
          });

          // Cache the generated virtuals
          virtualBusesCache.set(cacheKey, {
            virtualEntities,
            timestamp: Date.now()
          });

          console.log(`[/api/buses] Generated ${virtualEntities.length} virtual buses from ${processedVirtualBlocks.size} blocks`);
        }
      }
    }

    // Combine real + virtual
    const allVehicleEntities = [...processedVehicles, ...virtualEntities];

    // Build final response
    const responseTime = Date.now() - startTime;

    if (noVirtuals) {
      // Raw GTFS-RT format (only real vehicles)
      res.json({
        header: vehicleResult.data?.header || { gtfsRealtimeVersion: "2.0", incrementality: "FULL_DATASET", timestamp: currentTimeSec },
        entity: processedVehicles
      });
    } else {
      // Full combined format with all three feeds
      const response = {
        metadata: {
          operatorId,
          location: operatorId === '36' ? 'Revelstoke' :
                    operatorId === '47' ? 'Kelowna' :
                    operatorId === '48' ? 'Victoria' : `Operator ${operatorId}`,
          fetchedAt: new Date().toISOString(),
          responseTimeMs: responseTime,
          no_virtuals: noVirtuals,
          all_virtuals_mode: allVirtuals,
          cacheInfo: {
            virtualBusesCached: virtualEntities.length > 0 && virtualBusesCache.has(`buses-${operatorId}-${allVirtuals}`) &&
                              (Date.now() - virtualBusesCache.get(`buses-${operatorId}-${allVirtuals}`).timestamp < 5000),
            realBusesFresh: true
          },
          feeds: {
            vehicle_positions: {
              success: vehicleResult.success,
              entities: allVehicleEntities.length,
              virtual_vehicles: virtualEntities.length,
              real_vehicles: processedVehicles.length,
              url: vehicleResult.url
            },
            trip_updates: {
              success: tripResult.success,
              entities: processedTrips.length,
              url: tripResult.url
            },
            service_alerts: {
              success: alertsResult.success,
              entities: alertsResult.data?.entity?.length || 0,
              url: alertsResult.url
            }
          },
          block_id: {
            enabled: true,
            source: "parsed_from_trip_id",
            vehicles_with_blockId: allVehicleEntities.filter(e => !!e.vehicle?.trip?.blockId).length,
            trip_updates_with_blockId: processedTrips.filter(e => !!e.tripUpdate?.trip?.blockId).length
          }
        },
        data: {
          vehicle_positions: {
            header: vehicleResult.data?.header || { gtfsRealtimeVersion: "2.0", incrementality: "FULL_DATASET", timestamp: currentTimeSec },
            entity: allVehicleEntities
          },
          trip_updates: tripResult.success ? {
            header: tripResult.data.header,
            entity: processedTrips
          } : null,
          service_alerts: alertsResult.success ? alertsResult.data : null
        }
      };

      const errors = [];
      if (!vehicleResult.success) errors.push(`Vehicle positions: ${vehicleResult.error}`);
      if (!tripResult.success) errors.push(`Trip updates: ${tripResult.error}`);
      if (!alertsResult.success) errors.push(`Service alerts: ${alertsResult.error}`);
      if (errors.length > 0) response.metadata.errors = errors;

      console.log(`[${new Date().toISOString()}] /api/buses completed in ${responseTime}ms | Vehicles: ${allVehicleEntities.length} (real: ${processedVehicles.length}, virtual: ${virtualEntities.length})`);
      res.json(response);
    }
  } catch (error) {
    console.error('Error in /api/buses:', error);
    res.status(500).json({
      error: 'Failed to fetch combined feeds',
      details: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Debug endpoint to inspect virtual position calculation
app.get('/api/debug/virtuals-detail', async (req, res) => {
  try {
    const operatorId = req.query.operatorId || '36';
    const allVirtuals = req.query.all_virtuals === 'true';
    const currentTimeSec = Math.floor(Date.now() / 1000);
    const start = Date.now();

    console.log(`[DEBUG-VIRTUALS] Called | operator=${operatorId} | all=${allVirtuals} | time=${currentTimeSec}`);

    const tripResult = await fetchGTFSFeed('tripupdates.pb', operatorId);
    if (!tripResult.success) return res.status(503).json({ error: 'Trip feed unavailable' });

    await ensureScheduleLoaded();
    const schedule = scheduleLoader.scheduleData;
    if (!schedule?.tripsMap || !schedule?.shapes || !schedule?.stops) {
      return res.status(500).json({ error: 'Schedule incomplete' });
    }

    const debugInfo = {
      currentTime: {
        unix: currentTimeSec,
        iso: new Date(currentTimeSec * 1000).toISOString()
      },
      scheduleStats: {
        tripsCount: Object.keys(schedule.tripsMap).length,
        shapesCount: Object.keys(schedule.shapes).length,
        stopsCount: Object.keys(schedule.stops).length,
        sampleTripKeys: Object.keys(schedule.tripsMap).slice(0, 3),  // Shows how trips are keyed
        sampleShapeKeys: Object.keys(schedule.shapes).slice(0, 3)   // Shows shape IDs
      },
      rtTrips: tripResult.data.entity.map(e => e.tripUpdate?.trip?.tripId).filter(Boolean),
      virtualsDebug: []
    };

    tripResult.data.entity?.forEach(entity => {
      const tu = entity.tripUpdate;
      if (!tu?.trip?.tripId || !tu.stopTimeUpdate?.length) return;

      const trip = tu.trip;
      const stopTimes = tu.stopTimeUpdate;
      const blockId = extractBlockIdFromTripId(trip.tripId);

      const active = isTripCurrentlyActive(stopTimes, currentTimeSec);
      if (!active && !allVirtuals) return;  // Skip inactive unless all

      const info = findCurrentSegmentAndProgress(stopTimes, currentTimeSec);

      const shapeId = getShapeIdFromTrip(trip.tripId, schedule);

      let position = null;
      let shapeUsed = false;
      let fallbackReason = null;

      if (info) {
        const { currentStop, nextStop, progress } = info;
        const scheduleTimeSec = getScheduleTimeFromUnix(currentTimeSec, operatorId);
        position = calculateVirtualBusPosition(trip.tripId, scheduleTimeSec, schedule);
        shapeUsed = true;
      } else {
        fallbackReason = 'No stop info';
      }

      debugInfo.virtualsDebug.push({
        rtTripId: trip.tripId,
        blockId,
        routeId: trip.routeId,
        directionId: trip.directionId,
        startTime: trip.startTime,
        isActive: active,
        progress: info?.progress.toFixed(4) || 0,
        currentStop: info?.currentStop.stopId || 'none',
        nextStop: info?.nextStop?.stopId || 'none',
        shapeId,
        shapeUsed,
        fallbackReason,
        position
      });
    });

    const tookMs = Date.now() - start;

    res.json(debugInfo);
  } catch (err) {
    console.error('[DEBUG-VIRTUALS] Error:', err);
    res.status(500).json({ error: 'Debug failed', details: err.message });
  }
});
// Drop into Bus-Proxy-Revelstoke/server/server.js
// immediately before the existing app.get('/api/health', ...) route.
// Redeploy the Vercel/Railway proxy after adding this.

function setToArray(value) {
  if (!value) return [];
  if (value instanceof Set) return [...value];
  if (Array.isArray(value)) return value;
  if (typeof value === 'object') return Object.keys(value);
  return [];
}

function firstTripDeparture(stopTimes = []) {
  let best = null;
  for (const st of stopTimes) {
    const seq = Number(st.stop_sequence);
    const time = st.departure_time || st.arrival_time || '';
    if (!time) continue;
    if (!best || (Number.isFinite(seq) && seq < best.seq)) {
      best = { seq: Number.isFinite(seq) ? seq : 9999, time };
    }
  }
  return best ? best.time : '';
}

function buildGtfsBundleFromSchedule(schedule) {
  const tripsMap = schedule?.tripsMap || {};
  const stopsSrc = schedule?.stops || {};
  const stopTimesByTrip = schedule?.stopTimesByTrip || {};
  const calendarDates = schedule?.calendarDates || {};

  const stops = {};
  Object.entries(stopsSrc).forEach(([id, stop]) => {
    const lat = Number(stop?.lat);
    const lng = Number(stop?.lng != null ? stop.lng : stop?.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
    stops[String(id)] = {
      name: stop?.name || ('Stop ' + id),
      lat,
      lng
    };
  });

  const map = {};
  const blocks = {};
  Object.values(tripsMap).forEach((trip) => {
    const tripId = trip?.trip_id || trip?.tripId;
    const routeId = trip?.route_id || trip?.routeId;
    const blockId = String(trip?.block_id || trip?.blockId || '').trim();
    const stopTimes = (tripId && stopTimesByTrip[tripId]) || [];

    stopTimes.forEach((st) => {
      const stopId = String(st.stop_id || st.stopId || '').trim();
      if (!stopId || !routeId) return;
      if (!map[stopId]) map[stopId] = [];
      if (!map[stopId].includes(routeId)) map[stopId].push(routeId);
    });

    if (!blockId) return;
    if (!blocks[blockId]) blocks[blockId] = [];
    blocks[blockId].push({
      tripId: tripId || '',
      routeId: routeId || '',
      serviceId: trip?.service_id || trip?.serviceId || '',
      headsign: trip?.trip_headsign || trip?.tripHeadsign || 'Unknown destination',
      directionId: trip?.direction_id === '' || trip?.direction_id == null
        ? null
        : Number(trip.direction_id),
      startTime: firstTripDeparture(stopTimes)
    });
  });

  Object.keys(map).forEach((stopId) => map[stopId].sort());
  Object.keys(blocks).forEach((blockId) => {
    blocks[blockId].sort((a, b) => String(a.startTime).localeCompare(String(b.startTime)));
  });

  const servicesByDate = {};
  Object.entries(calendarDates).forEach(([serviceId, entry]) => {
    const added = setToArray(entry?.added || entry?.allDates || entry);
    added.forEach((date) => {
      if (!date) return;
      if (!servicesByDate[date]) servicesByDate[date] = [];
      if (!servicesByDate[date].includes(serviceId)) servicesByDate[date].push(serviceId);
    });
  });

  return {
    fetchedAt: new Date().toISOString(),
    source: 'scheduleLoader',
    map,
    stops,
    blocks,
    servicesByDate
  };
}

const GTFS_ZIP_URL = 'https://bct.tmix.se/Tmix.Cap.TdExport.WebApi/gtfs/?operatorIds=36';

app.get('/api/gtfs-bundle', async (req, res) => {
  try {
    if (!scheduleLoader?.scheduleData?.tripsMap) {
      await scheduleLoader.loadSchedules();
    }
    const bundle = buildGtfsBundleFromSchedule(scheduleLoader.scheduleData);
    res.set('Cache-Control', 'public, max-age=300');
    res.json(bundle);
  } catch (err) {
    console.error('[GTFS-BUNDLE]', err);
    res.status(500).json({ error: 'Failed to build GTFS bundle', details: err.message });
  }
});

app.get('/api/gtfs-zip', async (req, res) => {
  try {
    const upstream = await fetch(GTFS_ZIP_URL);
    if (!upstream.ok) {
      return res.status(upstream.status).send('Upstream GTFS zip failed');
    }
    const buffer = Buffer.from(await upstream.arrayBuffer());
    res.set({
      'Content-Type': 'application/zip',
      'Cache-Control': 'public, max-age=300',
      'Access-Control-Allow-Origin': '*'
    });
    res.send(buffer);
  } catch (err) {
    console.error('[GTFS-ZIP]', err);
    res.status(502).json({ error: 'Failed to proxy GTFS zip', details: err.message });
  }
});
// Health check
app.get('/api/health', (req, res) => {
  res.json({
    status: 'healthy',
    proto: !!root,
    schedule: !!scheduleLoader.scheduleData?.tripsMap,
    trips: scheduleLoader.scheduleData?.tripsMap ? Object.keys(scheduleLoader.scheduleData.tripsMap).length : 0,
    calendarDates: scheduleLoader.scheduleData?.calendarDates ? Object.keys(scheduleLoader.scheduleData.calendarDates).length : 0
  });
});

// Root page
app.get('/', (req, res) => {
  res.send('BC Transit GTFS-RT Proxy - Use /api/virtuals or /api/buses');
});

await loadProto();

export default app;

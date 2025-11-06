-- schema.sql
-- Important: drop in FK order to allow re-run
DROP TABLE IF EXISTS stop_events CASCADE;
DROP TABLE IF EXISTS trips CASCADE;
DROP TABLE IF EXISTS line_stops CASCADE;
DROP TABLE IF EXISTS stops CASCADE;
DROP TABLE IF EXISTS lines CASCADE;

-- Lines (route) master
CREATE TABLE lines (
  line_id SERIAL PRIMARY KEY,
  line_name VARCHAR(50) NOT NULL UNIQUE,         -- unique human name
  vehicle_type VARCHAR(10) NOT NULL
    CHECK (vehicle_type IN ('rail', 'bus'))      -- simple type guard
);

-- Stops master
CREATE TABLE stops (
  stop_id SERIAL PRIMARY KEY,
  stop_name VARCHAR(100) NOT NULL UNIQUE,        -- unique stop name
  latitude  DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL
);

-- Ordered stops per line (topology)
CREATE TABLE line_stops (
  line_id   INTEGER NOT NULL REFERENCES lines(line_id) ON DELETE CASCADE,
  stop_id   INTEGER NOT NULL REFERENCES stops(stop_id) ON DELETE CASCADE,
  sequence  INTEGER NOT NULL CHECK (sequence >= 1),   -- 1-based order
  time_offset INTEGER NOT NULL CHECK (time_offset >= 0), -- minutes from start
  PRIMARY KEY (line_id, stop_id),
  UNIQUE (line_id, sequence)                         -- one position per line
);

-- Trips scheduled for a line
CREATE TABLE trips (
  trip_id VARCHAR(32) PRIMARY KEY,                  -- use natural key from CSV
  line_id INTEGER NOT NULL REFERENCES lines(line_id) ON DELETE RESTRICT,
  scheduled_departure TIMESTAMP NOT NULL,
  vehicle_id VARCHAR(32) NOT NULL
);

-- Observed events at each stop for a trip
CREATE TABLE stop_events (
  trip_id VARCHAR(32) NOT NULL REFERENCES trips(trip_id) ON DELETE CASCADE,
  stop_id INTEGER NOT NULL REFERENCES stops(stop_id) ON DELETE CASCADE,
  scheduled TIMESTAMP NOT NULL,
  actual    TIMESTAMP NOT NULL,
  passengers_on  INTEGER NOT NULL CHECK (passengers_on  >= 0),
  passengers_off INTEGER NOT NULL CHECK (passengers_off >= 0),
  PRIMARY KEY (trip_id, stop_id)
);

-- ==== Performance indexes (important for <500ms queries) ====
CREATE INDEX IF NOT EXISTS idx_trips_line ON trips(line_id);
CREATE INDEX IF NOT EXISTS idx_line_stops_line_seq ON line_stops(line_id, sequence);
CREATE INDEX IF NOT EXISTS idx_stop_events_trip ON stop_events(trip_id);
CREATE INDEX IF NOT EXISTS idx_stop_events_stop ON stop_events(stop_id);
CREATE INDEX IF NOT EXISTS idx_stop_events_sched ON stop_events(scheduled);
CREATE INDEX IF NOT EXISTS idx_stop_events_actual ON stop_events(actual);
CREATE INDEX IF NOT EXISTS idx_stops_name ON stops(stop_name);
CREATE INDEX IF NOT EXISTS idx_lines_name ON lines(line_name);

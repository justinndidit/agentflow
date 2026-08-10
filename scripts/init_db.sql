-- init_db.sql
-- This script runs ONCE when PostgreSQL container first starts.
-- It creates necessary databases and enables extensions/functions in each.

CREATE DATABASE agentflow;

\c agentflowdb;

-- Enable UUID and text search extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
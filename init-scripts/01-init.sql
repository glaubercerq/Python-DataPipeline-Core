-- PostgreSQL initialization script for ETL Data Warehouse
-- This script runs when the PostgreSQL container starts for the first time

-- Create the database (this is handled by POSTGRES_DB env var, but we can add extensions here)
-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- The tables will be created by the ETL pipeline when it runs
-- This ensures the database is ready for the application
-- Création d'un keyspace (base de données)
CREATE KEYSPACE IF NOT EXISTS projeti2
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Utilisation du keyspace
USE projeti2;

-- Création d'une table
CREATE TABLE IF NOT EXISTS projeti2.soil_data (
    id UUID PRIMARY KEY,
    n INT,
    p INT,
    k INT,
    temperature DOUBLE,
    humidity DOUBLE,
    ph DOUBLE,
    rainfall DOUBLE,
    label TEXT
);

CREATE TABLE IF NOT EXISTS agriculture_data (
    farm_id TEXT,
    crop_type TEXT,
    farm_area_acres DOUBLE,
    irrigation_type TEXT,
    fertilizer_used_tons DOUBLE,
    pesticide_used_kg DOUBLE,
    yield_tons DOUBLE,
    soil_type TEXT,
    season TEXT,
    water_usage_cubic_meters DOUBLE,
    PRIMARY KEY (farm_id, crop_type, season)
);

CREATE TABLE IF NOT EXISTS projeti2.historical_climate (
    id UUID PRIMARY KEY,
    year INT,
    month INT,
    lat TEXT,
    lon_175_180W DOUBLE,
    lon_170_175W DOUBLE,
    lon_165_170W DOUBLE,
    lon_160_165W DOUBLE,
    lon_155_160W DOUBLE,
    lon_150_155W DOUBLE,
    lon_145_150W DOUBLE,
    lon_140_145E DOUBLE,
    lon_145_150E DOUBLE,
    lon_150_155E DOUBLE,
    lon_155_160E DOUBLE,
    lon_160_165E DOUBLE,
    lon_165_170E DOUBLE,
    lon_170_175E DOUBLE,
    lon_175_180E DOUBLE
);

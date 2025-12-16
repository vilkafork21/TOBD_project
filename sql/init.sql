-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Raw data schema: хранилище сырых данных
CREATE TABLE IF NOT EXISTS raw.energy_consumption (
    id SERIAL PRIMARY KEY,
    lclid VARCHAR(50),
    day DATE,
    energy_kwh DECIMAL(10, 4),
    timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Analytics schema: хранилище агрегированных данных
CREATE TABLE IF NOT EXISTS analytics.hourly_consumption (
    id SERIAL PRIMARY KEY,
    hour TIMESTAMP,
    total_consumption DECIMAL(12, 4),
    avg_consumption DECIMAL(10, 4),
    max_consumption DECIMAL(10, 4),
    min_consumption DECIMAL(10, 4),
    household_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.daily_consumption (
    id SERIAL PRIMARY KEY,
    day DATE,
    total_consumption DECIMAL(12, 4),
    avg_consumption DECIMAL(10, 4),
    max_consumption DECIMAL(10, 4),
    min_consumption DECIMAL(10, 4),
    household_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.weekly_consumption (
    id SERIAL PRIMARY KEY,
    week_start DATE,
    total_consumption DECIMAL(12, 4),
    avg_consumption DECIMAL(10, 4),
    max_consumption DECIMAL(10, 4),
    min_consumption DECIMAL(10, 4),
    household_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.monthly_consumption (
    id SERIAL PRIMARY KEY,
    month DATE,
    total_consumption DECIMAL(12, 4),
    avg_consumption DECIMAL(10, 4),
    max_consumption DECIMAL(10, 4),
    min_consumption DECIMAL(10, 4),
    household_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.household_stats (
    id SERIAL PRIMARY KEY,
    lclid VARCHAR(50),
    total_consumption DECIMAL(12, 4),
    avg_daily_consumption DECIMAL(10, 4),
    max_daily_consumption DECIMAL(10, 4),
    min_daily_consumption DECIMAL(10, 4),
    days_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.anomalies (
    id SERIAL PRIMARY KEY,
    lclid VARCHAR(50),
    timestamp TIMESTAMP,
    energy_kwh DECIMAL(10, 4),
    z_score DECIMAL(10, 4),
    anomaly_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_raw_day ON raw.energy_consumption(day);
CREATE INDEX IF NOT EXISTS idx_raw_timestamp ON raw.energy_consumption(timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_lclid ON raw.energy_consumption(lclid);

CREATE INDEX IF NOT EXISTS idx_hourly_hour ON analytics.hourly_consumption(hour);
CREATE INDEX IF NOT EXISTS idx_daily_day ON analytics.daily_consumption(day);
CREATE INDEX IF NOT EXISTS idx_weekly_week_start ON analytics.weekly_consumption(week_start);
CREATE INDEX IF NOT EXISTS idx_monthly_month ON analytics.monthly_consumption(month);
CREATE INDEX IF NOT EXISTS idx_household_lclid ON analytics.household_stats(lclid);
CREATE INDEX IF NOT EXISTS idx_anomalies_timestamp ON analytics.anomalies(timestamp);
CREATE INDEX IF NOT EXISTS idx_anomalies_lclid ON analytics.anomalies(lclid);


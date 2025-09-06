CREATE SCHEMA IF NOT EXISTS raw_amazon; -- one schema per source system
CREATE SCHEMA IF NOT EXISTS raw_youtube;
CREATE SCHEMA IF NOT EXISTS dev; -- for developing dbt models
CREATE SCHEMA IF NOT EXISTS prod_marts; -- for deploying dbt models to BI application(s)


-- SNCR - Database Schema

DROP TABLE IF EXISTS owners CASCADE;
DROP TABLE IF EXISTS properties CASCADE;
-- Rural properties
CREATE TABLE properties (
    incra_code VARCHAR(20) PRIMARY KEY,
    registration VARCHAR(20),
    municipality VARCHAR(100),
    state CHAR(2) NOT NULL,
    name VARCHAR(200),
    area_hectares NUMERIC(12, 2),
    status VARCHAR(50),
    acquisition_pct NUMERIC(6, 2)
);

-- Property owners and lessees
CREATE TABLE owners (
    id SERIAL PRIMARY KEY,
    incra_code VARCHAR(20) NOT NULL REFERENCES properties(incra_code),
    owner_name VARCHAR(200),
    cpf VARCHAR(20),
    relationship VARCHAR(50),
    participation_pct NUMERIC(6, 2),
    UNIQUE (incra_code, cpf)
);

-- Primary index: incra_code is already PK on properties (automatic B-tree index)
-- Foreign key index on owners for fast JOINs
CREATE INDEX idx_owners_incra_code ON owners(incra_code);

-- Indexes for analytical queries by region
CREATE INDEX idx_properties_state ON properties(state);
CREATE INDEX idx_properties_state_municipality ON properties(state, municipality);

-- Create ENUM types for fixed literals
CREATE TYPE measurement_type_enum AS ENUM ('ACC_X', 'ACC_Y', 'ACC_Z', 'BVP', 'EDA', 'HR', 'IBI', 'TEMP');
CREATE TYPE week_enum AS ENUM ('Week_1', 'Week_2');
CREATE TYPE patient_group_enum AS ENUM ('Exercise', 'VR');
CREATE TYPE origin_enum AS ENUM ('UMCG', 'Forte GGZ', 'Lentis', 'Argo GGZ', 'Mediant GGZ', 'Huisartsenpraktijk');

-- Table for patients
CREATE TABLE patient (
                         id TEXT PRIMARY KEY,
                         origin origin_enum,
                         patient_group  patient_group_enum
);

-- Table for measurements.
-- Each row represents one measurement type for a specific week and patient.
CREATE TABLE measurement (
                             id TEXT PRIMARY KEY,
                             patient_id TEXT REFERENCES patient(id),
                             week week_enum,
                             measurement_type measurement_type_enum,
                             sample_rate FLOAT NOT NULL
);

-- Table for measurement sessions.
-- The 'data' field is stored as JSONB to capture the list of samples.
CREATE TABLE measure_session (
                                 id SERIAL PRIMARY KEY,
                                 measurement_id TEXT REFERENCES measurement(id),
                                 start_timestamp TIMESTAMP,
                                 data FLOAT[]
);

-- Table for relaxation sessions
CREATE TABLE relax_session (
                               id SERIAL PRIMARY KEY,
                               patient_id TEXT REFERENCES patient(id),
                               start_timestamp TIMESTAMP,
                               end_timestamp TIMESTAMP
);

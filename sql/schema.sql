-- Healthcare Analytics Database Schema
-- PostgreSQL Database Schema for Healthcare ETL Pipeline

-- Create database (run this separately if needed)
-- CREATE DATABASE healthcare_analytics;

-- Set timezone
SET timezone = 'UTC';

-- Create schema for healthcare data
CREATE SCHEMA IF NOT EXISTS healthcare;

-- Use the healthcare schema
SET search_path TO healthcare, public;

-- =============================================
-- DIMENSION TABLES
-- =============================================

-- Patients dimension table
CREATE TABLE IF NOT EXISTS patients (
    patient_id INTEGER PRIMARY KEY,
    age INTEGER NOT NULL CHECK (age >= 0 AND age <= 120),
    gender VARCHAR(10) NOT NULL CHECK (gender IN ('Male', 'Female', 'Unknown')),
    race VARCHAR(20),
    zip_code VARCHAR(10),
    insurance_type VARCHAR(20),
    chronic_conditions INTEGER DEFAULT 0 CHECK (chronic_conditions >= 0),
    last_visit_date DATE,
    age_category VARCHAR(20),
    risk_level VARCHAR(20),
    days_since_last_visit INTEGER,
    patient_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Providers dimension table
CREATE TABLE IF NOT EXISTS providers (
    provider_id INTEGER PRIMARY KEY,
    hospital_name VARCHAR(100) NOT NULL,
    provider_type VARCHAR(50),
    state VARCHAR(2) NOT NULL,
    city VARCHAR(50),
    beds INTEGER CHECK (beds > 0),
    teaching_hospital BOOLEAN DEFAULT FALSE,
    hospital_size VARCHAR(20),
    full_address VARCHAR(100),
    avg_cost DECIMAL(10,2) DEFAULT 0.0,
    readmission_rate DECIMAL(5,4) DEFAULT 0.0,
    patient_volume INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Medications dimension table
CREATE TABLE IF NOT EXISTS medications (
    medication_id SERIAL PRIMARY KEY,
    medication_name VARCHAR(100) NOT NULL UNIQUE,
    medication_category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Diagnosis codes dimension table
CREATE TABLE IF NOT EXISTS diagnosis_codes (
    diagnosis_code VARCHAR(10) PRIMARY KEY,
    description TEXT,
    category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- FACT TABLES
-- =============================================

-- Claims fact table
CREATE TABLE IF NOT EXISTS claims (
    claim_id INTEGER PRIMARY KEY,
    patient_id INTEGER NOT NULL,
    provider_id INTEGER NOT NULL,
    diagnosis_code VARCHAR(10),
    procedure_code VARCHAR(10),
    admission_date DATE NOT NULL,
    discharge_date DATE,
    readmission_date DATE,
    readmission_flag INTEGER DEFAULT 0 CHECK (readmission_flag IN (0, 1)),
    cost DECIMAL(12,2) NOT NULL CHECK (cost > 0),
    insurance_type VARCHAR(20),
    length_of_stay INTEGER CHECK (length_of_stay >= 0),
    cost_per_day DECIMAL(10,2),
    cost_category VARCHAR(20),
    los_category VARCHAR(20),
    admission_month INTEGER,
    admission_quarter INTEGER,
    admission_year INTEGER,
    admission_dow VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    FOREIGN KEY (diagnosis_code) REFERENCES diagnosis_codes(diagnosis_code)
);

-- Prescriptions fact table
CREATE TABLE IF NOT EXISTS prescriptions (
    prescription_id INTEGER PRIMARY KEY,
    patient_id INTEGER NOT NULL,
    provider_id INTEGER NOT NULL,
    medication_id INTEGER,
    prescription_date DATE NOT NULL,
    days_supplied INTEGER NOT NULL CHECK (days_supplied > 0),
    days_prescribed INTEGER NOT NULL CHECK (days_prescribed > 0),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    cost DECIMAL(10,2) NOT NULL CHECK (cost > 0),
    adherence_rate DECIMAL(5,4),
    adherence_category VARCHAR(10),
    cost_per_day DECIMAL(10,2),
    prescription_month INTEGER,
    prescription_quarter INTEGER,
    prescription_year INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    FOREIGN KEY (medication_id) REFERENCES medications(medication_id)
);

-- =============================================
-- INDEXES FOR PERFORMANCE
-- =============================================

-- Claims table indexes
CREATE INDEX IF NOT EXISTS idx_claims_patient_id ON claims(patient_id);
CREATE INDEX IF NOT EXISTS idx_claims_provider_id ON claims(provider_id);
CREATE INDEX IF NOT EXISTS idx_claims_diagnosis_code ON claims(diagnosis_code);
CREATE INDEX IF NOT EXISTS idx_claims_admission_date ON claims(admission_date);
CREATE INDEX IF NOT EXISTS idx_claims_discharge_date ON claims(discharge_date);
CREATE INDEX IF NOT EXISTS idx_claims_cost ON claims(cost);
CREATE INDEX IF NOT EXISTS idx_claims_readmission_flag ON claims(readmission_flag);

-- Prescriptions table indexes
CREATE INDEX IF NOT EXISTS idx_prescriptions_patient_id ON prescriptions(patient_id);
CREATE INDEX IF NOT EXISTS idx_prescriptions_provider_id ON prescriptions(provider_id);
CREATE INDEX IF NOT EXISTS idx_prescriptions_medication_id ON prescriptions(medication_id);
CREATE INDEX IF NOT EXISTS idx_prescriptions_prescription_date ON prescriptions(prescription_date);
CREATE INDEX IF NOT EXISTS idx_prescriptions_adherence_rate ON prescriptions(adherence_rate);

-- Patients table indexes
CREATE INDEX IF NOT EXISTS idx_patients_age ON patients(age);
CREATE INDEX IF NOT EXISTS idx_patients_gender ON patients(gender);
CREATE INDEX IF NOT EXISTS idx_patients_insurance_type ON patients(insurance_type);
CREATE INDEX IF NOT EXISTS idx_patients_risk_level ON patients(risk_level);

-- Providers table indexes
CREATE INDEX IF NOT EXISTS idx_providers_state ON providers(state);
CREATE INDEX IF NOT EXISTS idx_providers_hospital_name ON providers(hospital_name);
CREATE INDEX IF NOT EXISTS idx_providers_hospital_size ON providers(hospital_size);

-- =============================================
-- VIEWS FOR COMMON QUERIES
-- =============================================

-- Patient summary view
CREATE OR REPLACE VIEW patient_summary AS
SELECT 
    p.patient_id,
    p.age,
    p.gender,
    p.race,
    p.insurance_type,
    p.chronic_conditions,
    p.risk_level,
    p.patient_status,
    COUNT(c.claim_id) as total_claims,
    SUM(c.cost) as total_cost,
    AVG(c.cost) as avg_cost_per_claim,
    MAX(c.admission_date) as last_admission_date,
    COUNT(CASE WHEN c.readmission_flag = 1 THEN 1 END) as readmissions
FROM patients p
LEFT JOIN claims c ON p.patient_id = c.patient_id
GROUP BY p.patient_id, p.age, p.gender, p.race, p.insurance_type, 
         p.chronic_conditions, p.risk_level, p.patient_status;

-- Provider performance view
CREATE OR REPLACE VIEW provider_performance AS
SELECT 
    pr.provider_id,
    pr.hospital_name,
    pr.state,
    pr.hospital_size,
    COUNT(c.claim_id) as total_claims,
    COUNT(DISTINCT c.patient_id) as unique_patients,
    SUM(c.cost) as total_revenue,
    AVG(c.cost) as avg_cost_per_claim,
    AVG(c.length_of_stay) as avg_length_of_stay,
    ROUND(AVG(c.readmission_flag::numeric), 4) as readmission_rate,
    COUNT(CASE WHEN c.readmission_flag = 1 THEN 1 END) as total_readmissions
FROM providers pr
LEFT JOIN claims c ON pr.provider_id = c.provider_id
GROUP BY pr.provider_id, pr.hospital_name, pr.state, pr.hospital_size;

-- Cost analysis by diagnosis view
CREATE OR REPLACE VIEW diagnosis_cost_analysis AS
SELECT 
    c.diagnosis_code,
    dc.description,
    dc.category,
    COUNT(c.claim_id) as claim_count,
    COUNT(DISTINCT c.patient_id) as unique_patients,
    SUM(c.cost) as total_cost,
    AVG(c.cost) as avg_cost_per_claim,
    ROUND(AVG(c.readmission_flag::numeric), 4) as readmission_rate,
    AVG(c.length_of_stay) as avg_length_of_stay
FROM claims c
LEFT JOIN diagnosis_codes dc ON c.diagnosis_code = dc.diagnosis_code
GROUP BY c.diagnosis_code, dc.description, dc.category
ORDER BY total_cost DESC;

-- =============================================
-- FUNCTIONS AND TRIGGERS
-- =============================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers to automatically update updated_at
CREATE TRIGGER update_patients_updated_at 
    BEFORE UPDATE ON patients 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_providers_updated_at 
    BEFORE UPDATE ON providers 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_claims_updated_at 
    BEFORE UPDATE ON claims 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_prescriptions_updated_at 
    BEFORE UPDATE ON prescriptions 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =============================================
-- SAMPLE DATA INSERTION
-- =============================================

-- Insert sample diagnosis codes
INSERT INTO diagnosis_codes (diagnosis_code, description, category) VALUES
('E11.9', 'Type 2 diabetes mellitus without complications', 'Endocrine'),
('I25.10', 'Atherosclerotic heart disease of native coronary artery without angina pectoris', 'Cardiovascular'),
('F32.9', 'Major depressive disorder, single episode, unspecified', 'Mental Health'),
('M79.3', 'Panniculitis, unspecified', 'Musculoskeletal'),
('K21.9', 'Gastro-esophageal reflux disease without esophagitis', 'Gastrointestinal'),
('G43.909', 'Migraine, unspecified, not intractable, without status migrainosus', 'Neurological'),
('M25.561', 'Pain in right knee', 'Musculoskeletal'),
('R06.02', 'Shortness of breath', 'Respiratory'),
('Z87.891', 'Personal history of nicotine dependence', 'Lifestyle'),
('I10', 'Essential hypertension', 'Cardiovascular'),
('E78.5', 'Hyperlipidemia, unspecified', 'Endocrine'),
('M54.5', 'Low back pain', 'Musculoskeletal'),
('R50.9', 'Fever, unspecified', 'General'),
('K59.00', 'Constipation, unspecified', 'Gastrointestinal')
ON CONFLICT (diagnosis_code) DO NOTHING;

-- Insert sample medications
INSERT INTO medications (medication_name, medication_category) VALUES
('Metformin', 'Diabetes'),
('Lisinopril', 'Cardiovascular'),
('Atorvastatin', 'Cardiovascular'),
('Metoprolol', 'Cardiovascular'),
('Omeprazole', 'Gastrointestinal'),
('Amlodipine', 'Cardiovascular'),
('Hydrochlorothiazide', 'Cardiovascular'),
('Simvastatin', 'Cardiovascular'),
('Losartan', 'Cardiovascular'),
('Albuterol', 'Respiratory')
ON CONFLICT (medication_name) DO NOTHING;

-- =============================================
-- GRANTS AND PERMISSIONS
-- =============================================

-- Create a read-only user for analytics
-- CREATE USER analytics_user WITH PASSWORD 'analytics_password';
-- GRANT USAGE ON SCHEMA healthcare TO analytics_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA healthcare TO analytics_user;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA healthcare TO analytics_user;

-- =============================================
-- COMMENTS
-- =============================================

COMMENT ON SCHEMA healthcare IS 'Healthcare analytics data warehouse schema';
COMMENT ON TABLE patients IS 'Patient demographic and clinical information';
COMMENT ON TABLE providers IS 'Healthcare provider and facility information';
COMMENT ON TABLE claims IS 'Healthcare claims and billing information';
COMMENT ON TABLE prescriptions IS 'Medication prescription and adherence data';
COMMENT ON TABLE medications IS 'Medication reference data';
COMMENT ON TABLE diagnosis_codes IS 'ICD-10 diagnosis code reference data';

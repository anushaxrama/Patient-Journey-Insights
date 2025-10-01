-- Healthcare Analytics Queries
-- Business intelligence queries for healthcare ETL pipeline

-- Set schema
SET search_path TO healthcare, public;

-- =============================================
-- COST ANALYSIS QUERIES
-- =============================================

-- 1. Top 10 Most Expensive Diagnosis Codes
-- Identifies the diagnosis codes that drive the highest costs
SELECT 
    c.diagnosis_code,
    dc.description,
    dc.category,
    COUNT(c.claim_id) as claim_count,
    COUNT(DISTINCT c.patient_id) as unique_patients,
    SUM(c.cost) as total_cost,
    ROUND(AVG(c.cost), 2) as avg_cost_per_claim,
    ROUND(SUM(c.cost) * 100.0 / SUM(SUM(c.cost)) OVER(), 2) as cost_percentage
FROM claims c
LEFT JOIN diagnosis_codes dc ON c.diagnosis_code = dc.diagnosis_code
GROUP BY c.diagnosis_code, dc.description, dc.category
ORDER BY total_cost DESC
LIMIT 10;

-- 2. Cost Analysis by Hospital
-- Shows cost performance by healthcare provider
SELECT 
    p.provider_id,
    p.hospital_name,
    p.state,
    p.hospital_size,
    COUNT(c.claim_id) as total_claims,
    COUNT(DISTINCT c.patient_id) as unique_patients,
    SUM(c.cost) as total_revenue,
    ROUND(AVG(c.cost), 2) as avg_cost_per_claim,
    ROUND(AVG(c.length_of_stay), 1) as avg_length_of_stay,
    ROUND(AVG(c.readmission_flag::numeric) * 100, 2) as readmission_rate_pct
FROM providers p
LEFT JOIN claims c ON p.provider_id = c.provider_id
GROUP BY p.provider_id, p.hospital_name, p.state, p.hospital_size
ORDER BY total_revenue DESC;

-- 3. Monthly Cost Trends
-- Shows cost trends over time
SELECT 
    admission_year,
    admission_month,
    COUNT(claim_id) as claim_count,
    SUM(cost) as total_cost,
    ROUND(AVG(cost), 2) as avg_cost_per_claim,
    ROUND(AVG(length_of_stay), 1) as avg_length_of_stay
FROM claims
GROUP BY admission_year, admission_month
ORDER BY admission_year, admission_month;

-- 4. Cost Distribution by Patient Demographics
-- Analyzes costs by patient age, gender, and insurance type
SELECT 
    p.age_category,
    p.gender,
    p.insurance_type,
    COUNT(c.claim_id) as claim_count,
    COUNT(DISTINCT c.patient_id) as unique_patients,
    SUM(c.cost) as total_cost,
    ROUND(AVG(c.cost), 2) as avg_cost_per_claim
FROM patients p
JOIN claims c ON p.patient_id = c.patient_id
GROUP BY p.age_category, p.gender, p.insurance_type
ORDER BY total_cost DESC;

-- =============================================
-- READMISSION ANALYSIS QUERIES
-- =============================================

-- 5. Readmission Rates by Hospital
-- Identifies hospitals with high readmission rates
SELECT 
    p.provider_id,
    p.hospital_name,
    p.state,
    COUNT(c.claim_id) as total_claims,
    COUNT(CASE WHEN c.readmission_flag = 1 THEN 1 END) as readmissions,
    ROUND(AVG(c.readmission_flag::numeric) * 100, 2) as readmission_rate_pct,
    ROUND(AVG(c.length_of_stay), 1) as avg_length_of_stay
FROM providers p
JOIN claims c ON p.provider_id = c.provider_id
GROUP BY p.provider_id, p.hospital_name, p.state
HAVING COUNT(c.claim_id) >= 10  -- Only hospitals with significant volume
ORDER BY readmission_rate_pct DESC;

-- 6. Readmission Analysis by Diagnosis
-- Shows which conditions have highest readmission rates
SELECT 
    c.diagnosis_code,
    dc.description,
    dc.category,
    COUNT(c.claim_id) as total_claims,
    COUNT(CASE WHEN c.readmission_flag = 1 THEN 1 END) as readmissions,
    ROUND(AVG(c.readmission_flag::numeric) * 100, 2) as readmission_rate_pct,
    ROUND(AVG(c.cost), 2) as avg_cost_per_claim
FROM claims c
LEFT JOIN diagnosis_codes dc ON c.diagnosis_code = dc.diagnosis_code
GROUP BY c.diagnosis_code, dc.description, dc.category
HAVING COUNT(c.claim_id) >= 5  -- Only conditions with sufficient data
ORDER BY readmission_rate_pct DESC;

-- 7. Readmission Risk by Patient Characteristics
-- Identifies patient risk factors for readmission
SELECT 
    p.age_category,
    p.gender,
    p.risk_level,
    p.chronic_conditions,
    COUNT(c.claim_id) as total_claims,
    COUNT(CASE WHEN c.readmission_flag = 1 THEN 1 END) as readmissions,
    ROUND(AVG(c.readmission_flag::numeric) * 100, 2) as readmission_rate_pct
FROM patients p
JOIN claims c ON p.patient_id = c.patient_id
GROUP BY p.age_category, p.gender, p.risk_level, p.chronic_conditions
ORDER BY readmission_rate_pct DESC;

-- =============================================
-- MEDICATION ADHERENCE QUERIES
-- =============================================

-- 8. Medication Adherence by Drug Category
-- Analyzes adherence rates by medication type
SELECT 
    m.medication_category,
    COUNT(pr.prescription_id) as total_prescriptions,
    COUNT(DISTINCT pr.patient_id) as unique_patients,
    ROUND(AVG(pr.adherence_rate) * 100, 2) as avg_adherence_pct,
    ROUND(AVG(pr.cost), 2) as avg_cost_per_prescription,
    SUM(pr.cost) as total_cost
FROM medications m
JOIN prescriptions pr ON m.medication_id = pr.medication_id
GROUP BY m.medication_category
ORDER BY avg_adherence_pct DESC;

-- 9. Patient Adherence Patterns
-- Shows adherence patterns by patient demographics
SELECT 
    p.age_category,
    p.gender,
    p.risk_level,
    COUNT(pr.prescription_id) as total_prescriptions,
    ROUND(AVG(pr.adherence_rate) * 100, 2) as avg_adherence_pct,
    COUNT(CASE WHEN pr.adherence_rate >= 0.8 THEN 1 END) as good_adherence_count,
    ROUND(COUNT(CASE WHEN pr.adherence_rate >= 0.8 THEN 1 END) * 100.0 / COUNT(pr.prescription_id), 2) as good_adherence_pct
FROM patients p
JOIN prescriptions pr ON p.patient_id = pr.patient_id
GROUP BY p.age_category, p.gender, p.risk_level
ORDER BY avg_adherence_pct DESC;

-- 10. Provider Prescribing Patterns
-- Analyzes prescribing patterns by healthcare provider
SELECT 
    p.provider_id,
    p.hospital_name,
    COUNT(pr.prescription_id) as total_prescriptions,
    COUNT(DISTINCT pr.patient_id) as unique_patients,
    ROUND(AVG(pr.adherence_rate) * 100, 2) as avg_adherence_pct,
    ROUND(AVG(pr.cost), 2) as avg_cost_per_prescription,
    SUM(pr.cost) as total_prescription_cost
FROM providers p
JOIN prescriptions pr ON p.provider_id = pr.provider_id
GROUP BY p.provider_id, p.hospital_name
ORDER BY total_prescriptions DESC;

-- =============================================
-- PATIENT JOURNEY QUERIES
-- =============================================

-- 11. Patient Journey Summary
-- Provides comprehensive patient journey analysis
SELECT 
    p.patient_id,
    p.age,
    p.gender,
    p.risk_level,
    p.chronic_conditions,
    COUNT(c.claim_id) as total_claims,
    SUM(c.cost) as total_healthcare_cost,
    ROUND(AVG(c.cost), 2) as avg_cost_per_claim,
    COUNT(CASE WHEN c.readmission_flag = 1 THEN 1 END) as readmissions,
    ROUND(AVG(c.readmission_flag::numeric) * 100, 2) as readmission_rate_pct,
    COUNT(pr.prescription_id) as total_prescriptions,
    ROUND(AVG(pr.adherence_rate) * 100, 2) as avg_adherence_pct,
    MIN(c.admission_date) as first_visit,
    MAX(c.admission_date) as last_visit
FROM patients p
LEFT JOIN claims c ON p.patient_id = c.patient_id
LEFT JOIN prescriptions pr ON p.patient_id = pr.patient_id
GROUP BY p.patient_id, p.age, p.gender, p.risk_level, p.chronic_conditions
ORDER BY total_healthcare_cost DESC;

-- 12. High-Cost Patient Identification
-- Identifies patients with highest healthcare costs
SELECT 
    p.patient_id,
    p.age,
    p.gender,
    p.risk_level,
    p.chronic_conditions,
    COUNT(c.claim_id) as total_claims,
    SUM(c.cost) as total_cost,
    ROUND(AVG(c.cost), 2) as avg_cost_per_claim,
    COUNT(CASE WHEN c.readmission_flag = 1 THEN 1 END) as readmissions,
    ROUND(SUM(c.cost) * 100.0 / SUM(SUM(c.cost)) OVER(), 4) as cost_percentage
FROM patients p
JOIN claims c ON p.patient_id = c.patient_id
GROUP BY p.patient_id, p.age, p.gender, p.risk_level, p.chronic_conditions
HAVING SUM(c.cost) > (SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_cost) 
                      FROM (SELECT SUM(cost) as total_cost FROM claims GROUP BY patient_id) subq)
ORDER BY total_cost DESC;

-- =============================================
-- OPERATIONAL METRICS QUERIES
-- =============================================

-- 13. Hospital Performance Dashboard
-- Key performance indicators for each hospital
SELECT 
    p.provider_id,
    p.hospital_name,
    p.state,
    p.hospital_size,
    p.beds,
    COUNT(c.claim_id) as total_claims,
    COUNT(DISTINCT c.patient_id) as unique_patients,
    SUM(c.cost) as total_revenue,
    ROUND(AVG(c.cost), 2) as avg_cost_per_claim,
    ROUND(AVG(c.length_of_stay), 1) as avg_length_of_stay,
    ROUND(AVG(c.readmission_flag::numeric) * 100, 2) as readmission_rate_pct,
    ROUND(COUNT(DISTINCT c.patient_id) * 1.0 / p.beds, 2) as patients_per_bed
FROM providers p
LEFT JOIN claims c ON p.provider_id = c.provider_id
GROUP BY p.provider_id, p.hospital_name, p.state, p.hospital_size, p.beds
ORDER BY total_revenue DESC;

-- 14. Seasonal Analysis
-- Analyzes seasonal patterns in healthcare utilization
SELECT 
    admission_quarter,
    admission_month,
    COUNT(claim_id) as claim_count,
    COUNT(DISTINCT patient_id) as unique_patients,
    SUM(cost) as total_cost,
    ROUND(AVG(cost), 2) as avg_cost_per_claim,
    ROUND(AVG(length_of_stay), 1) as avg_length_of_stay,
    ROUND(AVG(readmission_flag::numeric) * 100, 2) as readmission_rate_pct
FROM claims
GROUP BY admission_quarter, admission_month
ORDER BY admission_quarter, admission_month;

-- 15. Insurance Type Analysis
-- Analyzes healthcare utilization by insurance type
SELECT 
    insurance_type,
    COUNT(claim_id) as claim_count,
    COUNT(DISTINCT patient_id) as unique_patients,
    SUM(cost) as total_cost,
    ROUND(AVG(cost), 2) as avg_cost_per_claim,
    ROUND(AVG(length_of_stay), 1) as avg_length_of_stay,
    ROUND(AVG(readmission_flag::numeric) * 100, 2) as readmission_rate_pct
FROM claims
GROUP BY insurance_type
ORDER BY total_cost DESC;

-- =============================================
-- PREDICTIVE ANALYTICS QUERIES
-- =============================================

-- 16. Readmission Risk Scoring
-- Creates a simple risk score for readmission prediction
WITH patient_risk_scores AS (
    SELECT 
        p.patient_id,
        p.age,
        p.gender,
        p.chronic_conditions,
        p.risk_level,
        COUNT(c.claim_id) as total_claims,
        SUM(c.cost) as total_cost,
        COUNT(CASE WHEN c.readmission_flag = 1 THEN 1 END) as previous_readmissions,
        ROUND(AVG(c.length_of_stay), 1) as avg_length_of_stay,
        -- Simple risk score calculation
        (CASE WHEN p.age >= 65 THEN 2 ELSE 1 END) +
        (CASE WHEN p.chronic_conditions >= 3 THEN 3 ELSE p.chronic_conditions END) +
        (CASE WHEN COUNT(CASE WHEN c.readmission_flag = 1 THEN 1 END) > 0 THEN 2 ELSE 0 END) +
        (CASE WHEN AVG(c.length_of_stay) > 7 THEN 1 ELSE 0 END) as risk_score
    FROM patients p
    LEFT JOIN claims c ON p.patient_id = c.patient_id
    GROUP BY p.patient_id, p.age, p.gender, p.chronic_conditions, p.risk_level
)
SELECT 
    patient_id,
    age,
    gender,
    chronic_conditions,
    risk_level,
    total_claims,
    total_cost,
    previous_readmissions,
    avg_length_of_stay,
    risk_score,
    CASE 
        WHEN risk_score <= 3 THEN 'Low Risk'
        WHEN risk_score <= 6 THEN 'Medium Risk'
        ELSE 'High Risk'
    END as risk_category
FROM patient_risk_scores
ORDER BY risk_score DESC;

-- =============================================
-- COST OPTIMIZATION QUERIES
-- =============================================

-- 17. Cost Efficiency Analysis
-- Identifies opportunities for cost optimization
SELECT 
    c.diagnosis_code,
    dc.description,
    dc.category,
    COUNT(c.claim_id) as claim_count,
    SUM(c.cost) as total_cost,
    ROUND(AVG(c.cost), 2) as avg_cost_per_claim,
    ROUND(AVG(c.length_of_stay), 1) as avg_length_of_stay,
    ROUND(AVG(c.readmission_flag::numeric) * 100, 2) as readmission_rate_pct,
    -- Cost efficiency metric (lower is better)
    ROUND(AVG(c.cost) / NULLIF(AVG(c.length_of_stay), 0), 2) as cost_per_day,
    -- Potential savings if readmission rate reduced by 50%
    ROUND(SUM(c.cost) * AVG(c.readmission_flag::numeric) * 0.5, 2) as potential_savings
FROM claims c
LEFT JOIN diagnosis_codes dc ON c.diagnosis_code = dc.diagnosis_code
GROUP BY c.diagnosis_code, dc.description, dc.category
HAVING COUNT(c.claim_id) >= 5
ORDER BY potential_savings DESC;

-- 18. Provider Efficiency Comparison
-- Compares provider efficiency metrics
SELECT 
    p.provider_id,
    p.hospital_name,
    p.state,
    p.hospital_size,
    COUNT(c.claim_id) as total_claims,
    SUM(c.cost) as total_revenue,
    ROUND(AVG(c.cost), 2) as avg_cost_per_claim,
    ROUND(AVG(c.length_of_stay), 1) as avg_length_of_stay,
    ROUND(AVG(c.readmission_flag::numeric) * 100, 2) as readmission_rate_pct,
    -- Efficiency score (higher is better)
    ROUND(COUNT(c.claim_id) * 100.0 / NULLIF(AVG(c.readmission_flag::numeric), 0), 2) as efficiency_score
FROM providers p
JOIN claims c ON p.provider_id = c.provider_id
GROUP BY p.provider_id, p.hospital_name, p.state, p.hospital_size
HAVING COUNT(c.claim_id) >= 10
ORDER BY efficiency_score DESC;

-- =============================================
-- SUMMARY STATISTICS
-- =============================================

-- 19. Overall Healthcare System Summary
-- Provides high-level summary statistics
SELECT 
    'Total Patients' as metric,
    COUNT(DISTINCT patient_id)::text as value
FROM patients
UNION ALL
SELECT 
    'Total Providers' as metric,
    COUNT(DISTINCT provider_id)::text as value
FROM providers
UNION ALL
SELECT 
    'Total Claims' as metric,
    COUNT(claim_id)::text as value
FROM claims
UNION ALL
SELECT 
    'Total Prescriptions' as metric,
    COUNT(prescription_id)::text as value
FROM prescriptions
UNION ALL
SELECT 
    'Total Healthcare Cost' as metric,
    '$' || ROUND(SUM(cost), 2)::text as value
FROM claims
UNION ALL
SELECT 
    'Average Cost per Claim' as metric,
    '$' || ROUND(AVG(cost), 2)::text as value
FROM claims
UNION ALL
SELECT 
    'Overall Readmission Rate' as metric,
    ROUND(AVG(readmission_flag::numeric) * 100, 2)::text || '%' as value
FROM claims
UNION ALL
SELECT 
    'Average Length of Stay' as metric,
    ROUND(AVG(length_of_stay), 1)::text || ' days' as value
FROM claims;

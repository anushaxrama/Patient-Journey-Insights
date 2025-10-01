"""
Generate Sample Healthcare Data
Creates realistic sample datasets for testing the healthcare ETL pipeline.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path

def generate_sample_claims_data(n_records=10000):
    """Generate sample claims data."""
    print(f"Generating {n_records} sample claims records...")
    
    np.random.seed(42)
    
    # Sample data
    data = {
        'claim_id': range(1, n_records + 1),
        'patient_id': np.random.randint(1, 5001, n_records),
        'provider_id': np.random.randint(1, 101, n_records),
        'admission_date': pd.date_range('2020-01-01', '2023-12-31', periods=n_records),
        'discharge_date': pd.date_range('2020-01-02', '2024-01-01', periods=n_records),
        'diagnosis_code': np.random.choice([
            'E11.9', 'I25.10', 'F32.9', 'M79.3', 'K21.9', 'G43.909', 'M25.561',
            'R06.02', 'Z87.891', 'I10', 'E78.5', 'M54.5', 'R50.9', 'K59.00',
            'J44.1', 'N18.6', 'I48.91', 'G47.00', 'M79.3', 'R10.9'
        ], n_records),
        'procedure_code': np.random.choice([
            '99213', '99214', '99215', '99281', '99282', '99283', '99284', '99285',
            '99201', '99202', '99203', '99204', '99205', '99211', '99212'
        ], n_records),
        'cost': np.random.exponential(5000, n_records),
        'insurance_type': np.random.choice(['Medicare', 'Medicaid', 'Private', 'Self-Pay'], n_records),
        'length_of_stay': np.random.poisson(3, n_records)
    }
    
    df = pd.DataFrame(data)
    
    # Add some readmissions
    readmission_indices = np.random.choice(n_records, size=int(n_records * 0.15), replace=False)
    df.loc[readmission_indices, 'readmission_date'] = df.loc[readmission_indices, 'discharge_date'] + pd.Timedelta(days=np.random.randint(1, 30))
    
    return df

def generate_sample_patients_data(n_patients=5000):
    """Generate sample patients data."""
    print(f"Generating {n_patients} sample patient records...")
    
    np.random.seed(42)
    
    data = {
        'patient_id': range(1, n_patients + 1),
        'age': np.random.randint(18, 100, n_patients),
        'gender': np.random.choice(['Male', 'Female'], n_patients),
        'race': np.random.choice(['White', 'Black', 'Hispanic', 'Asian', 'Other'], n_patients),
        'zip_code': np.random.randint(10000, 99999, n_patients),
        'insurance_type': np.random.choice(['Medicare', 'Medicaid', 'Private', 'Self-Pay'], n_patients),
        'chronic_conditions': np.random.choice([0, 1, 2, 3, 4, 5], n_patients),
        'last_visit_date': pd.date_range('2020-01-01', '2023-12-31', periods=n_patients)
    }
    
    return pd.DataFrame(data)

def generate_sample_providers_data(n_providers=100):
    """Generate sample providers data."""
    print(f"Generating {n_providers} sample provider records...")
    
    np.random.seed(42)
    
    hospital_names = [
        'General Hospital', 'City Medical Center', 'Regional Health System',
        'University Hospital', 'Community Health Center', 'Metro General',
        'St. Mary\'s Hospital', 'Children\'s Hospital', 'Memorial Medical',
        'Valley Regional Hospital', 'Central Medical', 'Northside Hospital',
        'Southwest Medical', 'Eastside Health', 'Westside Medical Center'
    ]
    
    states = ['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI', 'VA', 'WA', 'AZ', 'CO', 'TN']
    
    data = {
        'provider_id': range(1, n_providers + 1),
        'hospital_name': np.random.choice(hospital_names, n_providers),
        'provider_type': np.random.choice(['Hospital', 'Clinic', 'Emergency', 'Specialty'], n_providers),
        'state': np.random.choice(states, n_providers),
        'city': [f'City_{i}' for i in range(1, n_providers + 1)],
        'beds': np.random.randint(50, 1000, n_providers),
        'teaching_hospital': np.random.choice([True, False], n_providers)
    }
    
    return pd.DataFrame(data)

def generate_sample_prescriptions_data(n_prescriptions=15000):
    """Generate sample prescriptions data."""
    print(f"Generating {n_prescriptions} sample prescription records...")
    
    np.random.seed(42)
    
    medications = [
        'Metformin', 'Lisinopril', 'Atorvastatin', 'Metoprolol', 'Omeprazole',
        'Amlodipine', 'Hydrochlorothiazide', 'Simvastatin', 'Losartan', 'Albuterol',
        'Levothyroxine', 'Gabapentin', 'Hydrocodone', 'Tramadol', 'Furosemide',
        'Pantoprazole', 'Sertraline', 'Trazodone', 'Montelukast', 'Clonazepam'
    ]
    
    data = {
        'prescription_id': range(1, n_prescriptions + 1),
        'patient_id': np.random.randint(1, 5001, n_prescriptions),
        'provider_id': np.random.randint(1, 101, n_prescriptions),
        'medication_name': np.random.choice(medications, n_prescriptions),
        'prescription_date': pd.date_range('2020-01-01', '2023-12-31', periods=n_prescriptions),
        'days_supplied': np.random.randint(7, 90, n_prescriptions),
        'days_prescribed': np.random.randint(7, 90, n_prescriptions),
        'quantity': np.random.randint(30, 500, n_prescriptions),
        'cost': np.random.exponential(50, n_prescriptions)
    }
    
    return pd.DataFrame(data)

def generate_sample_diagnosis_codes():
    """Generate sample diagnosis codes data."""
    print("Generating sample diagnosis codes...")
    
    diagnosis_codes = [
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
        ('K59.00', 'Constipation, unspecified', 'Gastrointestinal'),
        ('J44.1', 'Chronic obstructive pulmonary disease with acute exacerbation', 'Respiratory'),
        ('N18.6', 'End stage renal disease', 'Renal'),
        ('I48.91', 'Unspecified atrial fibrillation', 'Cardiovascular'),
        ('G47.00', 'Insomnia, unspecified', 'Neurological'),
        ('R10.9', 'Unspecified abdominal pain', 'Gastrointestinal'),
        ('M25.511', 'Pain in right shoulder', 'Musculoskeletal')
    ]
    
    df = pd.DataFrame(diagnosis_codes, columns=['diagnosis_code', 'description', 'category'])
    return df

def generate_sample_medications():
    """Generate sample medications data."""
    print("Generating sample medications...")
    
    medications = [
        ('Metformin', 'Diabetes'),
        ('Lisinopril', 'Cardiovascular'),
        ('Atorvastatin', 'Cardiovascular'),
        ('Metoprolol', 'Cardiovascular'),
        ('Omeprazole', 'Gastrointestinal'),
        ('Amlodipine', 'Cardiovascular'),
        ('Hydrochlorothiazide', 'Cardiovascular'),
        ('Simvastatin', 'Cardiovascular'),
        ('Losartan', 'Cardiovascular'),
        ('Albuterol', 'Respiratory'),
        ('Levothyroxine', 'Endocrine'),
        ('Gabapentin', 'Neurological'),
        ('Hydrocodone', 'Pain Management'),
        ('Tramadol', 'Pain Management'),
        ('Furosemide', 'Cardiovascular'),
        ('Pantoprazole', 'Gastrointestinal'),
        ('Sertraline', 'Mental Health'),
        ('Trazodone', 'Mental Health'),
        ('Montelukast', 'Respiratory'),
        ('Clonazepam', 'Neurological')
    ]
    
    df = pd.DataFrame(medications, columns=['medication_name', 'medication_category'])
    return df

def save_sample_data():
    """Generate and save all sample data."""
    print("🏥 Generating Sample Healthcare Data")
    print("=" * 40)
    
    # Create data directory
    data_dir = Path("data/sample")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate and save claims data
    claims_df = generate_sample_claims_data()
    claims_df.to_csv(data_dir / "claims_sample.csv", index=False)
    print(f"✅ Saved claims data: {len(claims_df)} records")
    
    # Generate and save patients data
    patients_df = generate_sample_patients_data()
    patients_df.to_csv(data_dir / "patients_sample.csv", index=False)
    print(f"✅ Saved patients data: {len(patients_df)} records")
    
    # Generate and save providers data
    providers_df = generate_sample_providers_data()
    providers_df.to_csv(data_dir / "providers_sample.csv", index=False)
    print(f"✅ Saved providers data: {len(providers_df)} records")
    
    # Generate and save prescriptions data
    prescriptions_df = generate_sample_prescriptions_data()
    prescriptions_df.to_csv(data_dir / "prescriptions_sample.csv", index=False)
    print(f"✅ Saved prescriptions data: {len(prescriptions_df)} records")
    
    # Generate and save diagnosis codes
    diagnosis_df = generate_sample_diagnosis_codes()
    diagnosis_df.to_csv(data_dir / "diagnosis_codes_sample.csv", index=False)
    print(f"✅ Saved diagnosis codes: {len(diagnosis_df)} records")
    
    # Generate and save medications
    medications_df = generate_sample_medications()
    medications_df.to_csv(data_dir / "medications_sample.csv", index=False)
    print(f"✅ Saved medications: {len(medications_df)} records")
    
    # Create a summary file
    summary = {
        'dataset': ['claims', 'patients', 'providers', 'prescriptions', 'diagnosis_codes', 'medications'],
        'records': [len(claims_df), len(patients_df), len(providers_df), 
                   len(prescriptions_df), len(diagnosis_df), len(medications_df)],
        'file': ['claims_sample.csv', 'patients_sample.csv', 'providers_sample.csv',
                'prescriptions_sample.csv', 'diagnosis_codes_sample.csv', 'medications_sample.csv']
    }
    
    summary_df = pd.DataFrame(summary)
    summary_df.to_csv(data_dir / "data_summary.csv", index=False)
    
    print("\n📊 Data Summary:")
    print(summary_df.to_string(index=False))
    
    print(f"\n🎉 Sample data generation completed!")
    print(f"📁 All files saved to: {data_dir}")
    print("\n💡 You can now use these files to test the ETL pipeline:")
    print("   python src/extract.py --use-sample-data")

def main():
    """Main function to generate sample data."""
    save_sample_data()

if __name__ == "__main__":
    main()

"""
Data extraction module for healthcare ETL pipeline.
Extracts data from various sources and saves to bronze layer.
"""

import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import requests
from loguru import logger

from utils import (
    setup_logging, get_data_paths, ensure_directory_exists,
    log_dataframe_info, save_parquet_with_metadata, generate_etl_metadata
)

class HealthcareDataExtractor:
    """Extract healthcare data from various sources."""
    
    def __init__(self):
        setup_logging()
        self.data_paths = get_data_paths()
        self.bronze_path = self.data_paths["bronze"]
        ensure_directory_exists(self.bronze_path)
        
    def extract_claims_data(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """Extract claims data from CSV file or generate sample data."""
        logger.info("Starting claims data extraction...")
        
        if file_path and os.path.exists(file_path):
            df = pd.read_csv(file_path)
            logger.info(f"Loaded claims data from {file_path}")
        else:
            # Generate sample claims data
            df = self._generate_sample_claims_data()
            logger.info("Generated sample claims data")
        
        # Add extraction metadata
        metadata = generate_etl_metadata()
        metadata.update({
            "source": file_path or "generated",
            "extraction_timestamp": datetime.now().isoformat(),
            "pipeline_stage": "bronze"
        })
        
        # Save to bronze layer
        output_path = os.path.join(self.bronze_path, "claims_raw.parquet")
        save_parquet_with_metadata(df, output_path, metadata)
        
        log_dataframe_info(df, "extract", "claims")
        return df
    
    def extract_patients_data(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """Extract patients data from CSV file or generate sample data."""
        logger.info("Starting patients data extraction...")
        
        if file_path and os.path.exists(file_path):
            df = pd.read_csv(file_path)
            logger.info(f"Loaded patients data from {file_path}")
        else:
            # Generate sample patients data
            df = self._generate_sample_patients_data()
            logger.info("Generated sample patients data")
        
        # Add extraction metadata
        metadata = generate_etl_metadata()
        metadata.update({
            "source": file_path or "generated",
            "extraction_timestamp": datetime.now().isoformat(),
            "pipeline_stage": "bronze"
        })
        
        # Save to bronze layer
        output_path = os.path.join(self.bronze_path, "patients_raw.parquet")
        save_parquet_with_metadata(df, output_path, metadata)
        
        log_dataframe_info(df, "extract", "patients")
        return df
    
    def extract_providers_data(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """Extract providers data from CSV file or generate sample data."""
        logger.info("Starting providers data extraction...")
        
        if file_path and os.path.exists(file_path):
            df = pd.read_csv(file_path)
            logger.info(f"Loaded providers data from {file_path}")
        else:
            # Generate sample providers data
            df = self._generate_sample_providers_data()
            logger.info("Generated sample providers data")
        
        # Add extraction metadata
        metadata = generate_etl_metadata()
        metadata.update({
            "source": file_path or "generated",
            "extraction_timestamp": datetime.now().isoformat(),
            "pipeline_stage": "bronze"
        })
        
        # Save to bronze layer
        output_path = os.path.join(self.bronze_path, "providers_raw.parquet")
        save_parquet_with_metadata(df, output_path, metadata)
        
        log_dataframe_info(df, "extract", "providers")
        return df
    
    def extract_prescriptions_data(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """Extract prescriptions data from CSV file or generate sample data."""
        logger.info("Starting prescriptions data extraction...")
        
        if file_path and os.path.exists(file_path):
            df = pd.read_csv(file_path)
            logger.info(f"Loaded prescriptions data from {file_path}")
        else:
            # Generate sample prescriptions data
            df = self._generate_sample_prescriptions_data()
            logger.info("Generated sample prescriptions data")
        
        # Add extraction metadata
        metadata = generate_etl_metadata()
        metadata.update({
            "source": file_path or "generated",
            "extraction_timestamp": datetime.now().isoformat(),
            "pipeline_stage": "bronze"
        })
        
        # Save to bronze layer
        output_path = os.path.join(self.bronze_path, "prescriptions_raw.parquet")
        save_parquet_with_metadata(df, output_path, metadata)
        
        log_dataframe_info(df, "extract", "prescriptions")
        return df
    
    def _generate_sample_claims_data(self) -> pd.DataFrame:
        """Generate sample claims data for demonstration."""
        import numpy as np
        
        np.random.seed(42)
        n_records = 10000
        
        # Sample data
        data = {
            'claim_id': range(1, n_records + 1),
            'patient_id': np.random.randint(1, 5001, n_records),
            'provider_id': np.random.randint(1, 101, n_records),
            'admission_date': pd.date_range('2020-01-01', '2023-12-31', periods=n_records),
            'discharge_date': pd.date_range('2020-01-02', '2024-01-01', periods=n_records),
            'diagnosis_code': np.random.choice([
                'E11.9', 'I25.10', 'F32.9', 'M79.3', 'K21.9', 'G43.909', 'M25.561',
                'R06.02', 'Z87.891', 'I10', 'E78.5', 'M54.5', 'R50.9', 'K59.00'
            ], n_records),
            'procedure_code': np.random.choice([
                '99213', '99214', '99215', '99281', '99282', '99283', '99284', '99285'
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
    
    def _generate_sample_patients_data(self) -> pd.DataFrame:
        """Generate sample patients data for demonstration."""
        import numpy as np
        
        np.random.seed(42)
        n_patients = 5000
        
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
    
    def _generate_sample_providers_data(self) -> pd.DataFrame:
        """Generate sample providers data for demonstration."""
        import numpy as np
        
        np.random.seed(42)
        n_providers = 100
        
        hospital_names = [
            'General Hospital', 'City Medical Center', 'Regional Health System',
            'University Hospital', 'Community Health Center', 'Metro General',
            'St. Mary\'s Hospital', 'Children\'s Hospital', 'Memorial Medical',
            'Valley Regional Hospital'
        ]
        
        states = ['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']
        
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
    
    def _generate_sample_prescriptions_data(self) -> pd.DataFrame:
        """Generate sample prescriptions data for demonstration."""
        import numpy as np
        
        np.random.seed(42)
        n_prescriptions = 15000
        
        medications = [
            'Metformin', 'Lisinopril', 'Atorvastatin', 'Metoprolol', 'Omeprazole',
            'Amlodipine', 'Hydrochlorothiazide', 'Simvastatin', 'Losartan', 'Albuterol'
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
    
    def extract_all_data(self, data_sources: Dict[str, str] = None) -> Dict[str, pd.DataFrame]:
        """Extract all data sources."""
        logger.info("Starting full data extraction...")
        
        if data_sources is None:
            data_sources = {}
        
        results = {}
        
        # Extract each data source
        results['claims'] = self.extract_claims_data(data_sources.get('claims'))
        results['patients'] = self.extract_patients_data(data_sources.get('patients'))
        results['providers'] = self.extract_providers_data(data_sources.get('providers'))
        results['prescriptions'] = self.extract_prescriptions_data(data_sources.get('prescriptions'))
        
        logger.info("Data extraction completed successfully")
        return results

def main():
    """Main function to run data extraction."""
    extractor = HealthcareDataExtractor()
    
    # Extract all data
    data = extractor.extract_all_data()
    
    print(f"Extraction completed. Data saved to bronze layer:")
    for table_name, df in data.items():
        print(f"  {table_name}: {df.shape[0]} rows, {df.shape[1]} columns")

if __name__ == "__main__":
    main()

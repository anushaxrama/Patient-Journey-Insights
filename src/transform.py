"""
Data transformation module for healthcare ETL pipeline.
Cleans, standardizes, and enriches data from bronze to silver layer.
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import numpy as np
from loguru import logger

from utils import (
    setup_logging, get_data_paths, ensure_directory_exists,
    log_dataframe_info, save_parquet_with_metadata, load_parquet_with_metadata,
    generate_etl_metadata, clean_icd_code, calculate_readmission_flag,
    calculate_medication_adherence, standardize_gender, validate_dataframe
)

class HealthcareDataTransformer:
    """Transform healthcare data from bronze to silver layer."""
    
    def __init__(self):
        setup_logging()
        self.data_paths = get_data_paths()
        self.bronze_path = self.data_paths["bronze"]
        self.silver_path = self.data_paths["silver"]
        ensure_directory_exists(self.silver_path)
        
    def transform_claims_data(self) -> pd.DataFrame:
        """Transform claims data from bronze to silver layer."""
        logger.info("Starting claims data transformation...")
        
        # Load raw claims data
        input_path = os.path.join(self.bronze_path, "claims_raw.parquet")
        df, metadata = load_parquet_with_metadata(input_path)
        
        # Data cleaning and validation
        df = self._clean_claims_data(df)
        
        # Feature engineering
        df = self._engineer_claims_features(df)
        
        # Data quality checks
        self._validate_claims_data(df)
        
        # Save transformed data
        output_metadata = generate_etl_metadata()
        output_metadata.update({
            "source_metadata": metadata,
            "transformation_timestamp": datetime.now().isoformat(),
            "pipeline_stage": "silver",
            "transformation_version": "1.0.0"
        })
        
        output_path = os.path.join(self.silver_path, "claims_clean.parquet")
        save_parquet_with_metadata(df, output_path, output_metadata)
        
        log_dataframe_info(df, "transform", "claims")
        return df
    
    def transform_patients_data(self) -> pd.DataFrame:
        """Transform patients data from bronze to silver layer."""
        logger.info("Starting patients data transformation...")
        
        # Load raw patients data
        input_path = os.path.join(self.bronze_path, "patients_raw.parquet")
        df, metadata = load_parquet_with_metadata(input_path)
        
        # Data cleaning and validation
        df = self._clean_patients_data(df)
        
        # Feature engineering
        df = self._engineer_patients_features(df)
        
        # Data quality checks
        self._validate_patients_data(df)
        
        # Save transformed data
        output_metadata = generate_etl_metadata()
        output_metadata.update({
            "source_metadata": metadata,
            "transformation_timestamp": datetime.now().isoformat(),
            "pipeline_stage": "silver",
            "transformation_version": "1.0.0"
        })
        
        output_path = os.path.join(self.silver_path, "patients_clean.parquet")
        save_parquet_with_metadata(df, output_path, output_metadata)
        
        log_dataframe_info(df, "transform", "patients")
        return df
    
    def transform_providers_data(self) -> pd.DataFrame:
        """Transform providers data from bronze to silver layer."""
        logger.info("Starting providers data transformation...")
        
        # Load raw providers data
        input_path = os.path.join(self.bronze_path, "providers_raw.parquet")
        df, metadata = load_parquet_with_metadata(input_path)
        
        # Data cleaning and validation
        df = self._clean_providers_data(df)
        
        # Feature engineering
        df = self._engineer_providers_features(df)
        
        # Data quality checks
        self._validate_providers_data(df)
        
        # Save transformed data
        output_metadata = generate_etl_metadata()
        output_metadata.update({
            "source_metadata": metadata,
            "transformation_timestamp": datetime.now().isoformat(),
            "pipeline_stage": "silver",
            "transformation_version": "1.0.0"
        })
        
        output_path = os.path.join(self.silver_path, "providers_clean.parquet")
        save_parquet_with_metadata(df, output_path, output_metadata)
        
        log_dataframe_info(df, "transform", "providers")
        return df
    
    def transform_prescriptions_data(self) -> pd.DataFrame:
        """Transform prescriptions data from bronze to silver layer."""
        logger.info("Starting prescriptions data transformation...")
        
        # Load raw prescriptions data
        input_path = os.path.join(self.bronze_path, "prescriptions_raw.parquet")
        df, metadata = load_parquet_with_metadata(input_path)
        
        # Data cleaning and validation
        df = self._clean_prescriptions_data(df)
        
        # Feature engineering
        df = self._engineer_prescriptions_features(df)
        
        # Data quality checks
        self._validate_prescriptions_data(df)
        
        # Save transformed data
        output_metadata = generate_etl_metadata()
        output_metadata.update({
            "source_metadata": metadata,
            "transformation_timestamp": datetime.now().isoformat(),
            "pipeline_stage": "silver",
            "transformation_version": "1.0.0"
        })
        
        output_path = os.path.join(self.silver_path, "prescriptions_clean.parquet")
        save_parquet_with_metadata(df, output_path, output_metadata)
        
        log_dataframe_info(df, "transform", "prescriptions")
        return df
    
    def _clean_claims_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean claims data."""
        logger.info("Cleaning claims data...")
        
        # Remove duplicates
        initial_rows = len(df)
        df = df.drop_duplicates(subset=['claim_id'])
        logger.info(f"Removed {initial_rows - len(df)} duplicate claims")
        
        # Clean diagnosis codes
        df['diagnosis_code'] = df['diagnosis_code'].apply(clean_icd_code)
        
        # Clean procedure codes
        df['procedure_code'] = df['procedure_code'].str.upper().str.strip()
        
        # Handle missing values
        df['insurance_type'] = df['insurance_type'].fillna('Unknown')
        
        # Convert dates
        date_columns = ['admission_date', 'discharge_date', 'readmission_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Ensure cost is numeric and positive
        df['cost'] = pd.to_numeric(df['cost'], errors='coerce')
        df = df[df['cost'] > 0]  # Remove invalid costs
        
        # Ensure length of stay is reasonable
        df['length_of_stay'] = pd.to_numeric(df['length_of_stay'], errors='coerce')
        df = df[(df['length_of_stay'] >= 0) & (df['length_of_stay'] <= 365)]
        
        logger.info(f"Claims data cleaned: {len(df)} records remaining")
        return df
    
    def _clean_patients_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean patients data."""
        logger.info("Cleaning patients data...")
        
        # Remove duplicates
        initial_rows = len(df)
        df = df.drop_duplicates(subset=['patient_id'])
        logger.info(f"Removed {initial_rows - len(df)} duplicate patients")
        
        # Standardize gender
        df['gender'] = standardize_gender(df['gender'])
        
        # Clean age
        df['age'] = pd.to_numeric(df['age'], errors='coerce')
        df = df[(df['age'] >= 0) & (df['age'] <= 120)]  # Reasonable age range
        
        # Clean zip codes
        df['zip_code'] = df['zip_code'].astype(str).str[:5]  # Keep first 5 digits
        
        # Handle missing values
        df['race'] = df['race'].fillna('Unknown')
        df['insurance_type'] = df['insurance_type'].fillna('Unknown')
        
        # Convert dates
        df['last_visit_date'] = pd.to_datetime(df['last_visit_date'], errors='coerce')
        
        logger.info(f"Patients data cleaned: {len(df)} records remaining")
        return df
    
    def _clean_providers_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean providers data."""
        logger.info("Cleaning providers data...")
        
        # Remove duplicates
        initial_rows = len(df)
        df = df.drop_duplicates(subset=['provider_id'])
        logger.info(f"Removed {initial_rows - len(df)} duplicate providers")
        
        # Clean text fields
        df['hospital_name'] = df['hospital_name'].str.strip().str.title()
        df['provider_type'] = df['provider_type'].str.strip().str.title()
        df['state'] = df['state'].str.upper().str.strip()
        df['city'] = df['city'].str.strip().str.title()
        
        # Clean numeric fields
        df['beds'] = pd.to_numeric(df['beds'], errors='coerce')
        df = df[df['beds'] > 0]  # Remove invalid bed counts
        
        # Handle missing values
        df['teaching_hospital'] = df['teaching_hospital'].fillna(False)
        
        logger.info(f"Providers data cleaned: {len(df)} records remaining")
        return df
    
    def _clean_prescriptions_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean prescriptions data."""
        logger.info("Cleaning prescriptions data...")
        
        # Remove duplicates
        initial_rows = len(df)
        df = df.drop_duplicates(subset=['prescription_id'])
        logger.info(f"Removed {initial_rows - len(df)} duplicate prescriptions")
        
        # Clean medication names
        df['medication_name'] = df['medication_name'].str.strip().str.title()
        
        # Convert dates
        df['prescription_date'] = pd.to_datetime(df['prescription_date'], errors='coerce')
        
        # Clean numeric fields
        numeric_columns = ['days_supplied', 'days_prescribed', 'quantity', 'cost']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df[df[col] > 0]  # Remove invalid values
        
        logger.info(f"Prescriptions data cleaned: {len(df)} records remaining")
        return df
    
    def _engineer_claims_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer features for claims data."""
        logger.info("Engineering claims features...")
        
        # Calculate readmission flag
        df['readmission_flag'] = calculate_readmission_flag(
            df['admission_date'], df['discharge_date'], df.get('readmission_date', pd.Series())
        )
        
        # Calculate cost per day
        df['cost_per_day'] = df['cost'] / (df['length_of_stay'] + 1)  # +1 to avoid division by zero
        
        # Categorize costs
        df['cost_category'] = pd.cut(
            df['cost'],
            bins=[0, 1000, 5000, 15000, float('inf')],
            labels=['Low', 'Medium', 'High', 'Very High']
        )
        
        # Categorize length of stay
        df['los_category'] = pd.cut(
            df['length_of_stay'],
            bins=[0, 1, 3, 7, float('inf')],
            labels=['Same Day', 'Short', 'Medium', 'Long']
        )
        
        # Add seasonal features
        df['admission_month'] = df['admission_date'].dt.month
        df['admission_quarter'] = df['admission_date'].dt.quarter
        df['admission_year'] = df['admission_date'].dt.year
        
        # Add day of week
        df['admission_dow'] = df['admission_date'].dt.day_name()
        
        logger.info("Claims features engineered successfully")
        return df
    
    def _engineer_patients_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer features for patients data."""
        logger.info("Engineering patients features...")
        
        # Age categories
        df['age_category'] = pd.cut(
            df['age'],
            bins=[0, 18, 35, 50, 65, 100],
            labels=['Pediatric', 'Young Adult', 'Adult', 'Middle Age', 'Senior']
        )
        
        # Risk stratification based on chronic conditions
        df['risk_level'] = pd.cut(
            df['chronic_conditions'],
            bins=[-1, 0, 2, 4, float('inf')],
            labels=['Low', 'Medium', 'High', 'Very High']
        )
        
        # Time since last visit
        current_date = datetime.now()
        df['days_since_last_visit'] = (current_date - df['last_visit_date']).dt.days
        
        # Patient status
        df['patient_status'] = df['days_since_last_visit'].apply(
            lambda x: 'Active' if x <= 90 else 'Inactive' if x <= 365 else 'Dormant'
        )
        
        logger.info("Patients features engineered successfully")
        return df
    
    def _engineer_providers_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer features for providers data."""
        logger.info("Engineering providers features...")
        
        # Hospital size categories
        df['hospital_size'] = pd.cut(
            df['beds'],
            bins=[0, 100, 300, 600, float('inf')],
            labels=['Small', 'Medium', 'Large', 'Very Large']
        )
        
        # Create full address
        df['full_address'] = df['city'] + ', ' + df['state']
        
        # Provider performance indicators (will be calculated later with claims data)
        df['avg_cost'] = 0.0
        df['readmission_rate'] = 0.0
        df['patient_volume'] = 0
        
        logger.info("Providers features engineered successfully")
        return df
    
    def _engineer_prescriptions_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer features for prescriptions data."""
        logger.info("Engineering prescriptions features...")
        
        # Calculate adherence
        df['adherence_rate'] = calculate_medication_adherence(
            df['days_supplied'], df['days_prescribed']
        )
        
        # Adherence categories
        df['adherence_category'] = pd.cut(
            df['adherence_rate'],
            bins=[0, 0.5, 0.8, 1.0],
            labels=['Poor', 'Fair', 'Good']
        )
        
        # Cost per day
        df['cost_per_day'] = df['cost'] / df['days_supplied']
        
        # Medication categories (simplified)
        medication_categories = {
            'Metformin': 'Diabetes', 'Lisinopril': 'Cardiovascular', 'Atorvastatin': 'Cardiovascular',
            'Metoprolol': 'Cardiovascular', 'Omeprazole': 'Gastrointestinal', 'Amlodipine': 'Cardiovascular',
            'Hydrochlorothiazide': 'Cardiovascular', 'Simvastatin': 'Cardiovascular',
            'Losartan': 'Cardiovascular', 'Albuterol': 'Respiratory'
        }
        df['medication_category'] = df['medication_name'].map(medication_categories).fillna('Other')
        
        # Prescription timing features
        df['prescription_month'] = df['prescription_date'].dt.month
        df['prescription_quarter'] = df['prescription_date'].dt.quarter
        df['prescription_year'] = df['prescription_date'].dt.year
        
        logger.info("Prescriptions features engineered successfully")
        return df
    
    def _validate_claims_data(self, df: pd.DataFrame) -> None:
        """Validate claims data quality."""
        required_columns = ['claim_id', 'patient_id', 'provider_id', 'diagnosis_code', 'cost']
        if not validate_dataframe(df, required_columns, 'claims'):
            raise ValueError("Claims data validation failed")
        
        # Check for reasonable cost ranges
        if df['cost'].min() <= 0:
            logger.warning("Found non-positive costs in claims data")
        
        if df['cost'].max() > 1000000:  # $1M threshold
            logger.warning("Found extremely high costs in claims data")
        
        logger.info("Claims data validation completed")
    
    def _validate_patients_data(self, df: pd.DataFrame) -> None:
        """Validate patients data quality."""
        required_columns = ['patient_id', 'age', 'gender']
        if not validate_dataframe(df, required_columns, 'patients'):
            raise ValueError("Patients data validation failed")
        
        # Check age ranges
        if df['age'].min() < 0 or df['age'].max() > 120:
            logger.warning("Found unreasonable ages in patients data")
        
        logger.info("Patients data validation completed")
    
    def _validate_providers_data(self, df: pd.DataFrame) -> None:
        """Validate providers data quality."""
        required_columns = ['provider_id', 'hospital_name', 'state']
        if not validate_dataframe(df, required_columns, 'providers'):
            raise ValueError("Providers data validation failed")
        
        logger.info("Providers data validation completed")
    
    def _validate_prescriptions_data(self, df: pd.DataFrame) -> None:
        """Validate prescriptions data quality."""
        required_columns = ['prescription_id', 'patient_id', 'medication_name', 'cost']
        if not validate_dataframe(df, required_columns, 'prescriptions'):
            raise ValueError("Prescriptions data validation failed")
        
        logger.info("Prescriptions data validation completed")
    
    def transform_all_data(self) -> Dict[str, pd.DataFrame]:
        """Transform all data sources from bronze to silver layer."""
        logger.info("Starting full data transformation...")
        
        results = {}
        
        # Transform each data source
        results['claims'] = self.transform_claims_data()
        results['patients'] = self.transform_patients_data()
        results['providers'] = self.transform_providers_data()
        results['prescriptions'] = self.transform_prescriptions_data()
        
        logger.info("Data transformation completed successfully")
        return results

def main():
    """Main function to run data transformation."""
    transformer = HealthcareDataTransformer()
    
    # Transform all data
    data = transformer.transform_all_data()
    
    print(f"Transformation completed. Data saved to silver layer:")
    for table_name, df in data.items():
        print(f"  {table_name}: {df.shape[0]} rows, {df.shape[1]} columns")

if __name__ == "__main__":
    main()

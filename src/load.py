"""
Data loading module for healthcare ETL pipeline.
Loads transformed data from silver layer into PostgreSQL database.
"""

import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from loguru import logger

from utils import (
    setup_logging, get_database_connection, get_data_paths, ensure_directory_exists,
    log_dataframe_info, load_parquet_with_metadata, generate_etl_metadata
)

class HealthcareDataLoader:
    """Load healthcare data from silver layer to PostgreSQL database."""
    
    def __init__(self):
        setup_logging()
        self.data_paths = get_data_paths()
        self.silver_path = self.data_paths["silver"]
        self.gold_path = self.data_paths["gold"]
        ensure_directory_exists(self.gold_path)
        
        # Database connection
        self.connection_string = get_database_connection()
        self.engine = None
        
    def connect_to_database(self) -> bool:
        """Establish connection to PostgreSQL database."""
        try:
            self.engine = create_engine(self.connection_string)
            
            # Test connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                logger.info("Successfully connected to PostgreSQL database")
                return True
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def create_schema(self) -> bool:
        """Create database schema and tables."""
        try:
            with self.engine.connect() as conn:
                # Read and execute schema file
                schema_file = os.path.join(os.path.dirname(__file__), '..', 'sql', 'schema.sql')
                with open(schema_file, 'r') as f:
                    schema_sql = f.read()
                
                # Execute schema creation
                conn.execute(text(schema_sql))
                conn.commit()
                logger.info("Database schema created successfully")
                return True
                
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            return False
    
    def load_patients_data(self) -> bool:
        """Load patients data to database."""
        logger.info("Loading patients data to database...")
        
        try:
            # Load transformed patients data
            input_path = os.path.join(self.silver_path, "patients_clean.parquet")
            df, metadata = load_parquet_with_metadata(input_path)
            
            # Prepare data for loading
            df = self._prepare_patients_for_db(df)
            
            # Load to database
            df.to_sql('patients', self.engine, if_exists='replace', index=False, method='multi')
            
            # Verify loading
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM healthcare.patients"))
                count = result.scalar()
                logger.info(f"Successfully loaded {count} patients to database")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load patients data: {e}")
            return False
    
    def load_providers_data(self) -> bool:
        """Load providers data to database."""
        logger.info("Loading providers data to database...")
        
        try:
            # Load transformed providers data
            input_path = os.path.join(self.silver_path, "providers_clean.parquet")
            df, metadata = load_parquet_with_metadata(input_path)
            
            # Prepare data for loading
            df = self._prepare_providers_for_db(df)
            
            # Load to database
            df.to_sql('providers', self.engine, if_exists='replace', index=False, method='multi')
            
            # Verify loading
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM healthcare.providers"))
                count = result.scalar()
                logger.info(f"Successfully loaded {count} providers to database")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load providers data: {e}")
            return False
    
    def load_claims_data(self) -> bool:
        """Load claims data to database."""
        logger.info("Loading claims data to database...")
        
        try:
            # Load transformed claims data
            input_path = os.path.join(self.silver_path, "claims_clean.parquet")
            df, metadata = load_parquet_with_metadata(input_path)
            
            # Prepare data for loading
            df = self._prepare_claims_for_db(df)
            
            # Load to database in chunks to handle large datasets
            chunk_size = 1000
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                if_exists = 'replace' if i == 0 else 'append'
                chunk.to_sql('claims', self.engine, if_exists=if_exists, index=False, method='multi')
            
            # Verify loading
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM healthcare.claims"))
                count = result.scalar()
                logger.info(f"Successfully loaded {count} claims to database")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load claims data: {e}")
            return False
    
    def load_prescriptions_data(self) -> bool:
        """Load prescriptions data to database."""
        logger.info("Loading prescriptions data to database...")
        
        try:
            # Load transformed prescriptions data
            input_path = os.path.join(self.silver_path, "prescriptions_clean.parquet")
            df, metadata = load_parquet_with_metadata(input_path)
            
            # Prepare data for loading
            df = self._prepare_prescriptions_for_db(df)
            
            # Load to database in chunks
            chunk_size = 1000
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                if_exists = 'replace' if i == 0 else 'append'
                chunk.to_sql('prescriptions', self.engine, if_exists=if_exists, index=False, method='multi')
            
            # Verify loading
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM healthcare.prescriptions"))
                count = result.scalar()
                logger.info(f"Successfully loaded {count} prescriptions to database")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load prescriptions data: {e}")
            return False
    
    def _prepare_patients_for_db(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare patients data for database loading."""
        # Ensure required columns exist
        required_columns = ['patient_id', 'age', 'gender']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"Missing required column: {col}")
                return df
        
        # Convert data types
        df['patient_id'] = df['patient_id'].astype('int64')
        df['age'] = df['age'].astype('int64')
        
        # Handle missing values
        df['race'] = df.get('race', 'Unknown')
        df['zip_code'] = df.get('zip_code', '00000')
        df['insurance_type'] = df.get('insurance_type', 'Unknown')
        df['chronic_conditions'] = df.get('chronic_conditions', 0).astype('int64')
        
        # Convert dates
        if 'last_visit_date' in df.columns:
            df['last_visit_date'] = pd.to_datetime(df['last_visit_date']).dt.date
        
        return df
    
    def _prepare_providers_for_db(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare providers data for database loading."""
        # Ensure required columns exist
        required_columns = ['provider_id', 'hospital_name', 'state']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"Missing required column: {col}")
                return df
        
        # Convert data types
        df['provider_id'] = df['provider_id'].astype('int64')
        df['beds'] = df.get('beds', 100).astype('int64')
        df['teaching_hospital'] = df.get('teaching_hospital', False).astype(bool)
        
        # Handle missing values
        df['provider_type'] = df.get('provider_type', 'Hospital')
        df['city'] = df.get('city', 'Unknown')
        
        return df
    
    def _prepare_claims_for_db(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare claims data for database loading."""
        # Ensure required columns exist
        required_columns = ['claim_id', 'patient_id', 'provider_id', 'cost']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"Missing required column: {col}")
                return df
        
        # Convert data types
        df['claim_id'] = df['claim_id'].astype('int64')
        df['patient_id'] = df['patient_id'].astype('int64')
        df['provider_id'] = df['provider_id'].astype('int64')
        df['cost'] = df['cost'].astype('float64')
        
        # Convert dates
        date_columns = ['admission_date', 'discharge_date', 'readmission_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.date
        
        # Handle missing values
        df['readmission_flag'] = df.get('readmission_flag', 0).astype('int64')
        df['length_of_stay'] = df.get('length_of_stay', 1).astype('int64')
        df['insurance_type'] = df.get('insurance_type', 'Unknown')
        
        return df
    
    def _prepare_prescriptions_for_db(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare prescriptions data for database loading."""
        # Ensure required columns exist
        required_columns = ['prescription_id', 'patient_id', 'medication_name', 'cost']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"Missing required column: {col}")
                return df
        
        # Convert data types
        df['prescription_id'] = df['prescription_id'].astype('int64')
        df['patient_id'] = df['patient_id'].astype('int64')
        df['provider_id'] = df['provider_id'].astype('int64')
        df['cost'] = df['cost'].astype('float64')
        df['days_supplied'] = df['days_supplied'].astype('int64')
        df['days_prescribed'] = df['days_prescribed'].astype('int64')
        df['quantity'] = df['quantity'].astype('int64')
        
        # Convert dates
        if 'prescription_date' in df.columns:
            df['prescription_date'] = pd.to_datetime(df['prescription_date']).dt.date
        
        # Get medication_id from medications table
        df = self._add_medication_ids(df)
        
        return df
    
    def _add_medication_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add medication_id by looking up medication names."""
        try:
            with self.engine.connect() as conn:
                # Get medication mapping
                result = conn.execute(text("SELECT medication_id, medication_name FROM healthcare.medications"))
                medication_map = {row[1]: row[0] for row in result}
                
                # Map medication names to IDs
                df['medication_id'] = df['medication_name'].map(medication_map)
                
                # Handle unmapped medications
                unmapped = df[df['medication_id'].isna()]['medication_name'].unique()
                if len(unmapped) > 0:
                    logger.warning(f"Found {len(unmapped)} unmapped medications: {unmapped}")
                    df['medication_id'] = df['medication_id'].fillna(0)  # Default to 0 for unknown
                
                return df
                
        except Exception as e:
            logger.error(f"Failed to add medication IDs: {e}")
            df['medication_id'] = 0
            return df
    
    def update_provider_metrics(self) -> bool:
        """Update provider performance metrics based on claims data."""
        logger.info("Updating provider performance metrics...")
        
        try:
            with self.engine.connect() as conn:
                # Update provider metrics
                update_sql = """
                UPDATE healthcare.providers 
                SET 
                    avg_cost = subq.avg_cost,
                    readmission_rate = subq.readmission_rate,
                    patient_volume = subq.patient_volume
                FROM (
                    SELECT 
                        provider_id,
                        AVG(cost) as avg_cost,
                        AVG(readmission_flag::numeric) as readmission_rate,
                        COUNT(DISTINCT patient_id) as patient_volume
                    FROM healthcare.claims
                    GROUP BY provider_id
                ) subq
                WHERE healthcare.providers.provider_id = subq.provider_id
                """
                
                conn.execute(text(update_sql))
                conn.commit()
                
                logger.info("Provider metrics updated successfully")
                return True
                
        except Exception as e:
            logger.error(f"Failed to update provider metrics: {e}")
            return False
    
    def create_analytics_views(self) -> bool:
        """Create analytics views for business intelligence."""
        logger.info("Creating analytics views...")
        
        try:
            with self.engine.connect() as conn:
                # Views are already created in schema.sql
                # This function can be used to create additional views if needed
                logger.info("Analytics views are ready")
                return True
                
        except Exception as e:
            logger.error(f"Failed to create analytics views: {e}")
            return False
    
    def load_all_data(self) -> bool:
        """Load all data sources to database."""
        logger.info("Starting full data loading process...")
        
        # Connect to database
        if not self.connect_to_database():
            return False
        
        # Create schema
        if not self.create_schema():
            return False
        
        # Load data in order (dimensions first, then facts)
        success = True
        
        if not self.load_patients_data():
            success = False
        
        if not self.load_providers_data():
            success = False
        
        if not self.load_claims_data():
            success = False
        
        if not self.load_prescriptions_data():
            success = False
        
        # Update derived metrics
        if success:
            self.update_provider_metrics()
            self.create_analytics_views()
        
        if success:
            logger.info("All data loaded successfully to database")
        else:
            logger.error("Some data loading operations failed")
        
        return success
    
    def verify_data_integrity(self) -> Dict[str, int]:
        """Verify data integrity by checking row counts."""
        logger.info("Verifying data integrity...")
        
        counts = {}
        
        try:
            with self.engine.connect() as conn:
                tables = ['patients', 'providers', 'claims', 'prescriptions']
                
                for table in tables:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM healthcare.{table}"))
                    count = result.scalar()
                    counts[table] = count
                    logger.info(f"{table}: {count} records")
                
                return counts
                
        except Exception as e:
            logger.error(f"Failed to verify data integrity: {e}")
            return {}

def main():
    """Main function to run data loading."""
    loader = HealthcareDataLoader()
    
    # Load all data
    success = loader.load_all_data()
    
    if success:
        print("Data loading completed successfully!")
        
        # Verify data integrity
        counts = loader.verify_data_integrity()
        print("\nData integrity verification:")
        for table, count in counts.items():
            print(f"  {table}: {count} records")
    else:
        print("Data loading failed. Check logs for details.")

if __name__ == "__main__":
    main()

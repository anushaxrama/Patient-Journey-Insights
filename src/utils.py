"""
Utility functions for the healthcare ETL pipeline.
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any
import pandas as pd
from loguru import logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def setup_logging(log_level: str = "INFO") -> None:
    """Setup logging configuration."""
    log_file = os.getenv("LOG_FILE", "./logs/etl_pipeline.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    logger.remove()  # Remove default handler
    logger.add(
        log_file,
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
        rotation="10 MB",
        retention="30 days"
    )
    logger.add(
        lambda msg: print(msg, end=""),
        level=log_level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}"
    )

def get_database_connection() -> str:
    """Get database connection string from environment variables."""
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "healthcare_analytics")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "")
    
    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

def get_data_paths() -> Dict[str, str]:
    """Get data storage paths from environment variables."""
    return {
        "bronze": os.getenv("BRONZE_PATH", "./data/bronze"),
        "silver": os.getenv("SILVER_PATH", "./data/silver"),
        "gold": os.getenv("GOLD_PATH", "./data/gold")
    }

def ensure_directory_exists(path: str) -> None:
    """Ensure directory exists, create if it doesn't."""
    os.makedirs(path, exist_ok=True)

def log_dataframe_info(df: pd.DataFrame, stage: str, table_name: str) -> None:
    """Log dataframe information for audit trail."""
    logger.info(f"{stage.upper()} - {table_name}: {df.shape[0]} rows, {df.shape[1]} columns")
    logger.info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Log null values
    null_counts = df.isnull().sum()
    if null_counts.sum() > 0:
        logger.warning(f"Null values found in {table_name}:")
        for col, count in null_counts[null_counts > 0].items():
            logger.warning(f"  {col}: {count} nulls")

def validate_dataframe(df: pd.DataFrame, required_columns: list, table_name: str) -> bool:
    """Validate dataframe has required columns."""
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        logger.error(f"Missing required columns in {table_name}: {missing_columns}")
        return False
    return True

def save_parquet_with_metadata(df: pd.DataFrame, file_path: str, metadata: Dict[str, Any] = None) -> None:
    """Save dataframe as parquet with metadata."""
    ensure_directory_exists(os.path.dirname(file_path))
    
    # Add metadata to dataframe
    if metadata:
        df.attrs.update(metadata)
    
    df.to_parquet(file_path, index=False)
    logger.info(f"Saved {df.shape[0]} rows to {file_path}")

def load_parquet_with_metadata(file_path: str) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """Load parquet file and return dataframe with metadata."""
    df = pd.read_parquet(file_path)
    metadata = df.attrs if hasattr(df, 'attrs') else {}
    return df, metadata

def generate_etl_metadata() -> Dict[str, Any]:
    """Generate standard ETL metadata."""
    return {
        "etl_timestamp": datetime.now().isoformat(),
        "etl_version": "1.0.0",
        "pipeline_stage": "unknown"
    }

def clean_icd_code(code: str) -> str:
    """Clean and standardize ICD codes."""
    if pd.isna(code):
        return None
    
    # Convert to string and clean
    code = str(code).upper().strip()
    
    # Remove any non-alphanumeric characters except dots
    code = ''.join(c for c in code if c.isalnum() or c == '.')
    
    # Ensure proper format (e.g., E11.9, I25.10)
    if len(code) > 0 and code[0].isalpha():
        if '.' not in code and len(code) > 3:
            code = code[:3] + '.' + code[3:]
    
    return code

def calculate_readmission_flag(admission_date: pd.Series, discharge_date: pd.Series, 
                             readmission_date: pd.Series, days_threshold: int = 30) -> pd.Series:
    """Calculate readmission flag based on time between discharge and readmission."""
    # Convert to datetime if not already
    admission_date = pd.to_datetime(admission_date, errors='coerce')
    discharge_date = pd.to_datetime(discharge_date, errors='coerce')
    readmission_date = pd.to_datetime(readmission_date, errors='coerce')
    
    # Calculate days between discharge and readmission
    days_between = (readmission_date - discharge_date).dt.days
    
    # Flag as readmission if within threshold
    readmission_flag = (days_between <= days_threshold) & (days_between > 0)
    
    return readmission_flag.fillna(False).astype(int)

def calculate_medication_adherence(days_supplied: pd.Series, days_prescribed: pd.Series) -> pd.Series:
    """Calculate medication adherence percentage."""
    # Handle division by zero and null values
    adherence = days_supplied / days_prescribed
    adherence = adherence.fillna(0)
    adherence = adherence.clip(0, 1)  # Cap at 100%
    
    return adherence

def standardize_gender(gender: pd.Series) -> pd.Series:
    """Standardize gender values."""
    gender_map = {
        'M': 'Male', 'F': 'Female', 'MALE': 'Male', 'FEMALE': 'Female',
        '1': 'Male', '0': 'Female', 'MALE': 'Male', 'FEMALE': 'Female'
    }
    
    return gender.str.upper().map(gender_map).fillna('Unknown')

def validate_date_range(start_date: str, end_date: str) -> bool:
    """Validate that start_date is before end_date."""
    try:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        return start <= end
    except:
        return False

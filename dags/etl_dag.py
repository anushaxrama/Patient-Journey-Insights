"""
Healthcare ETL Pipeline DAG
Apache Airflow DAG for orchestrating healthcare data processing pipeline.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
import os
import sys

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import ETL modules
from extract import HealthcareDataExtractor
from transform import HealthcareDataTransformer
from load import HealthcareDataLoader

# Default arguments for the DAG
default_args = {
    'owner': 'healthcare_analytics_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['analytics@healthcare.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Create the DAG
dag = DAG(
    'healthcare_etl_pipeline',
    default_args=default_args,
    description='Healthcare ETL Pipeline for Claims, Patients, Providers, and Prescriptions',
    schedule_interval='@daily',  # Run daily
    max_active_runs=1,
    tags=['healthcare', 'etl', 'analytics', 'claims', 'patients'],
)

# =============================================
# TASK DEFINITIONS
# =============================================

# Task 1: Check for new data files
check_data_files = FileSensor(
    task_id='check_data_files',
    filepath='/opt/airflow/data/input/',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300,
    dag=dag,
)

# Task 2: Extract data from sources
def extract_healthcare_data(**context):
    """Extract healthcare data from various sources."""
    extractor = HealthcareDataExtractor()
    
    # Extract all data sources
    data = extractor.extract_all_data()
    
    # Log extraction results
    for table_name, df in data.items():
        print(f"Extracted {table_name}: {df.shape[0]} rows, {df.shape[1]} columns")
    
    return {
        'extraction_status': 'success',
        'tables_extracted': list(data.keys()),
        'total_records': sum(df.shape[0] for df in data.values())
    }

extract_task = PythonOperator(
    task_id='extract_healthcare_data',
    python_callable=extract_healthcare_data,
    dag=dag,
)

# Task 3: Transform and clean data
def transform_healthcare_data(**context):
    """Transform and clean healthcare data."""
    transformer = HealthcareDataTransformer()
    
    # Transform all data sources
    data = transformer.transform_all_data()
    
    # Log transformation results
    for table_name, df in data.items():
        print(f"Transformed {table_name}: {df.shape[0]} rows, {df.shape[1]} columns")
    
    return {
        'transformation_status': 'success',
        'tables_transformed': list(data.keys()),
        'total_records': sum(df.shape[0] for df in data.values())
    }

transform_task = PythonOperator(
    task_id='transform_healthcare_data',
    python_callable=transform_healthcare_data,
    dag=dag,
)

# Task 4: Load data to database
def load_healthcare_data(**context):
    """Load healthcare data to PostgreSQL database."""
    loader = HealthcareDataLoader()
    
    # Load all data to database
    success = loader.load_all_data()
    
    if not success:
        raise Exception("Data loading failed")
    
    # Verify data integrity
    counts = loader.verify_data_integrity()
    
    return {
        'loading_status': 'success',
        'table_counts': counts
    }

load_task = PythonOperator(
    task_id='load_healthcare_data',
    python_callable=load_healthcare_data,
    dag=dag,
)

# Task 5: Run data quality checks
def run_data_quality_checks(**context):
    """Run comprehensive data quality checks."""
    from sqlalchemy import create_engine, text
    from utils import get_database_connection
    
    # Connect to database
    connection_string = get_database_connection()
    engine = create_engine(connection_string)
    
    quality_checks = []
    
    try:
        with engine.connect() as conn:
            # Check 1: Row count validation
            tables = ['patients', 'providers', 'claims', 'prescriptions']
            for table in tables:
                result = conn.execute(text(f"SELECT COUNT(*) FROM healthcare.{table}"))
                count = result.scalar()
                quality_checks.append({
                    'check': f'{table}_row_count',
                    'status': 'PASS' if count > 0 else 'FAIL',
                    'value': count
                })
            
            # Check 2: Data completeness
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_claims,
                    COUNT(CASE WHEN cost IS NULL OR cost <= 0 THEN 1 END) as invalid_costs,
                    COUNT(CASE WHEN admission_date IS NULL THEN 1 END) as missing_dates
                FROM healthcare.claims
            """))
            row = result.fetchone()
            
            quality_checks.append({
                'check': 'claims_data_completeness',
                'status': 'PASS' if row.invalid_costs == 0 and row.missing_dates == 0 else 'FAIL',
                'value': f"Invalid costs: {row.invalid_costs}, Missing dates: {row.missing_dates}"
            })
            
            # Check 3: Referential integrity
            result = conn.execute(text("""
                SELECT COUNT(*) as orphaned_claims
                FROM healthcare.claims c
                LEFT JOIN healthcare.patients p ON c.patient_id = p.patient_id
                WHERE p.patient_id IS NULL
            """))
            orphaned_claims = result.scalar()
            
            quality_checks.append({
                'check': 'referential_integrity',
                'status': 'PASS' if orphaned_claims == 0 else 'FAIL',
                'value': f"Orphaned claims: {orphaned_claims}"
            })
            
            # Check 4: Business rule validation
            result = conn.execute(text("""
                SELECT COUNT(*) as invalid_ages
                FROM healthcare.patients
                WHERE age < 0 OR age > 120
            """))
            invalid_ages = result.scalar()
            
            quality_checks.append({
                'check': 'age_validation',
                'status': 'PASS' if invalid_ages == 0 else 'FAIL',
                'value': f"Invalid ages: {invalid_ages}"
            })
    
    except Exception as e:
        quality_checks.append({
            'check': 'database_connection',
            'status': 'FAIL',
            'value': str(e)
        })
    
    # Log quality check results
    for check in quality_checks:
        print(f"Quality Check - {check['check']}: {check['status']} - {check['value']}")
    
    # Fail the task if any critical checks fail
    failed_checks = [check for check in quality_checks if check['status'] == 'FAIL']
    if failed_checks:
        raise Exception(f"Data quality checks failed: {failed_checks}")
    
    return {
        'quality_check_status': 'success',
        'checks_performed': len(quality_checks),
        'checks_passed': len([c for c in quality_checks if c['status'] == 'PASS'])
    }

quality_check_task = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
)

# Task 6: Generate analytics reports
def generate_analytics_reports(**context):
    """Generate analytics reports and insights."""
    from sqlalchemy import create_engine, text
    from utils import get_database_connection
    import pandas as pd
    
    # Connect to database
    connection_string = get_database_connection()
    engine = create_engine(connection_string)
    
    reports = {}
    
    try:
        with engine.connect() as conn:
            # Report 1: Top 10 Cost Drivers
            result = conn.execute(text("""
                SELECT 
                    diagnosis_code,
                    description,
                    total_cost,
                    claim_count,
                    avg_cost_per_claim
                FROM healthcare.diagnosis_cost_analysis
                LIMIT 10
            """))
            reports['top_cost_drivers'] = pd.DataFrame(result.fetchall(), columns=result.keys()).to_dict('records')
            
            # Report 2: Hospital Performance
            result = conn.execute(text("""
                SELECT 
                    hospital_name,
                    state,
                    total_claims,
                    total_revenue,
                    avg_cost_per_claim,
                    readmission_rate_pct
                FROM healthcare.provider_performance
                ORDER BY total_revenue DESC
                LIMIT 10
            """))
            reports['hospital_performance'] = pd.DataFrame(result.fetchall(), columns=result.keys()).to_dict('records')
            
            # Report 3: Readmission Analysis
            result = conn.execute(text("""
                SELECT 
                    hospital_name,
                    readmission_rate_pct,
                    total_claims,
                    total_readmissions
                FROM healthcare.provider_performance
                WHERE total_claims >= 10
                ORDER BY readmission_rate_pct DESC
                LIMIT 10
            """))
            reports['readmission_analysis'] = pd.DataFrame(result.fetchall(), columns=result.keys()).to_dict('records')
            
            # Report 4: Medication Adherence
            result = conn.execute(text("""
                SELECT 
                    medication_category,
                    total_prescriptions,
                    avg_adherence_pct,
                    total_cost
                FROM (
                    SELECT 
                        m.medication_category,
                        COUNT(pr.prescription_id) as total_prescriptions,
                        ROUND(AVG(pr.adherence_rate) * 100, 2) as avg_adherence_pct,
                        SUM(pr.cost) as total_cost
                    FROM healthcare.medications m
                    JOIN healthcare.prescriptions pr ON m.medication_id = pr.medication_id
                    GROUP BY m.medication_category
                ) subq
                ORDER BY avg_adherence_pct DESC
            """))
            reports['medication_adherence'] = pd.DataFrame(result.fetchall(), columns=result.keys()).to_dict('records')
    
    except Exception as e:
        print(f"Error generating reports: {e}")
        reports = {'error': str(e)}
    
    # Log report generation
    print(f"Generated {len(reports)} analytics reports")
    for report_name, data in reports.items():
        if isinstance(data, list):
            print(f"  {report_name}: {len(data)} records")
    
    return {
        'report_generation_status': 'success',
        'reports_generated': list(reports.keys()),
        'reports': reports
    }

analytics_task = PythonOperator(
    task_id='generate_analytics_reports',
    python_callable=generate_analytics_reports,
    dag=dag,
)

# Task 7: Send success notification
success_notification = EmailOperator(
    task_id='send_success_notification',
    to=['analytics@healthcare.com'],
    subject='Healthcare ETL Pipeline - Success',
    html_content="""
    <h2>Healthcare ETL Pipeline Completed Successfully</h2>
    <p>The daily healthcare ETL pipeline has completed successfully.</p>
    <p><strong>Execution Date:</strong> {{ ds }}</p>
    <p><strong>Pipeline Status:</strong> All tasks completed without errors</p>
    <p>Please check the Airflow UI for detailed logs and metrics.</p>
    """,
    dag=dag,
)

# Task 8: Send failure notification
failure_notification = EmailOperator(
    task_id='send_failure_notification',
    to=['analytics@healthcare.com', 'admin@healthcare.com'],
    subject='Healthcare ETL Pipeline - Failure',
    html_content="""
    <h2>Healthcare ETL Pipeline Failed</h2>
    <p>The daily healthcare ETL pipeline has failed.</p>
    <p><strong>Execution Date:</strong> {{ ds }}</p>
    <p><strong>Failed Task:</strong> {{ task_instance.task_id }}</p>
    <p>Please check the Airflow UI for detailed error logs.</p>
    """,
    dag=dag,
    trigger_rule='one_failed',
)

# =============================================
# TASK DEPENDENCIES
# =============================================

# Define task dependencies
extract_task >> transform_task >> load_task >> quality_check_task >> analytics_task

# Add notifications
analytics_task >> success_notification
[extract_task, transform_task, load_task, quality_check_task, analytics_task] >> failure_notification

# =============================================
# DAG DOCUMENTATION
# =============================================

dag.doc_md = """
# Healthcare ETL Pipeline

This DAG orchestrates the complete healthcare data processing pipeline:

## Pipeline Overview
1. **Extract**: Pull data from various healthcare data sources
2. **Transform**: Clean, standardize, and enrich the data
3. **Load**: Store processed data in PostgreSQL data warehouse
4. **Quality Check**: Validate data integrity and completeness
5. **Analytics**: Generate business intelligence reports

## Data Sources
- Healthcare Claims Data
- Patient Demographics
- Provider Information
- Prescription Records

## Schedule
- **Frequency**: Daily
- **Start Time**: 02:00 UTC
- **Retry Policy**: 2 retries with 5-minute delay

## Monitoring
- Email notifications on success/failure
- Data quality validation
- Performance metrics tracking

## Business Impact
- Enables cost analysis and optimization
- Supports readmission reduction initiatives
- Provides medication adherence insights
- Facilitates patient journey analysis
"""

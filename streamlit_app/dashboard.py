"""
Healthcare Analytics Dashboard
Interactive Streamlit dashboard for healthcare data analysis.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils import get_database_connection
from sqlalchemy import create_engine, text

# Page configuration
st.set_page_config(
    page_title="Healthcare Analytics Dashboard",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #1f77b4;
    }
    .sidebar .sidebar-content {
        background-color: #f8f9fa;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data_from_files():
    """Load data from parquet files (fallback when database is not available)."""
    try:
        # Load data from silver layer files
        claims_df = pd.read_parquet('data/silver/claims_clean.parquet')
        patients_df = pd.read_parquet('data/silver/patients_clean.parquet')
        providers_df = pd.read_parquet('data/silver/providers_clean.parquet')
        prescriptions_df = pd.read_parquet('data/silver/prescriptions_clean.parquet')
        
        # Create summary statistics
        summary_data = [
            {'metric': 'Total Patients', 'value': str(len(patients_df))},
            {'metric': 'Total Claims', 'value': str(len(claims_df))},
            {'metric': 'Total Providers', 'value': str(len(providers_df))},
            {'metric': 'Total Cost', 'value': f'${claims_df["cost"].sum():,.2f}'}
        ]
        summary_df = pd.DataFrame(summary_data)
        
        # Top cost drivers
        cost_drivers = claims_df.groupby('diagnosis_code').agg({
            'cost': ['sum', 'mean', 'count'],
            'readmission_flag': 'mean'
        }).round(2)
        cost_drivers.columns = ['total_cost', 'avg_cost_per_claim', 'claim_count', 'readmission_rate']
        cost_drivers = cost_drivers.reset_index()
        cost_drivers['description'] = 'Sample Diagnosis'
        cost_drivers = cost_drivers.sort_values('total_cost', ascending=False).head(10)
        
        # Hospital performance
        hospital_perf = claims_df.groupby('provider_id').agg({
            'cost': ['sum', 'mean'],
            'claim_id': 'count',
            'readmission_flag': 'mean'
        }).round(2)
        hospital_perf.columns = ['total_revenue', 'avg_cost_per_claim', 'total_claims', 'readmission_rate_pct']
        hospital_perf = hospital_perf.reset_index()
        hospital_perf = hospital_perf.merge(providers_df[['provider_id', 'hospital_name', 'state']], on='provider_id')
        hospital_perf = hospital_perf.sort_values('total_revenue', ascending=False).head(15)
        
        # Monthly trends
        claims_df_copy = claims_df.copy()
        claims_df_copy['admission_date'] = pd.to_datetime(claims_df_copy['admission_date'])
        monthly_trends = claims_df_copy.groupby([
            claims_df_copy['admission_date'].dt.year,
            claims_df_copy['admission_date'].dt.month
        ]).agg({
            'claim_id': 'count',
            'cost': ['sum', 'mean'],
            'length_of_stay': 'mean'
        }).round(2)
        monthly_trends.columns = ['claim_count', 'total_cost', 'avg_cost_per_claim', 'avg_length_of_stay']
        monthly_trends = monthly_trends.reset_index()
        monthly_trends.columns = ['admission_year', 'admission_month', 'claim_count', 'total_cost', 'avg_cost_per_claim', 'avg_length_of_stay']
        
        # Patient demographics
        patient_demo = patients_df.groupby(['age_category', 'gender', 'insurance_type']).agg({
            'patient_id': 'count',
            'chronic_conditions': 'mean'
        }).round(2)
        patient_demo.columns = ['patient_count', 'avg_chronic_conditions']
        patient_demo = patient_demo.reset_index()
        
        # Readmission analysis
        readmission_analysis = hospital_perf.copy()
        readmission_analysis['readmissions'] = (readmission_analysis['total_claims'] * readmission_analysis['readmission_rate_pct']).round(0)
        readmission_analysis = readmission_analysis[readmission_analysis['total_claims'] >= 10]
        readmission_analysis = readmission_analysis.sort_values('readmission_rate_pct', ascending=False).head(15)
        
        return {
            'summary': summary_df,
            'cost_drivers': cost_drivers,
            'hospital_performance': hospital_perf,
            'monthly_trends': monthly_trends,
            'patient_demographics': patient_demo,
            'readmission_analysis': readmission_analysis
        }
        
    except Exception as e:
        st.error(f"Error loading data from files: {e}")
        return None

@st.cache_data
def load_data_from_db():
    """Load data from PostgreSQL database."""
    try:
        connection_string = get_database_connection()
        engine = create_engine(connection_string)
        
        # Load key datasets
        with engine.connect() as conn:
            # Summary statistics
            summary_query = """
            SELECT 
                'Total Patients' as metric,
                COUNT(DISTINCT patient_id)::text as value
            FROM healthcare.patients
            UNION ALL
            SELECT 
                'Total Claims' as metric,
                COUNT(claim_id)::text as value
            FROM healthcare.claims
            UNION ALL
            SELECT 
                'Total Providers' as metric,
                COUNT(DISTINCT provider_id)::text as value
            FROM healthcare.providers
            UNION ALL
            SELECT 
                'Total Cost' as metric,
                '$' || ROUND(SUM(cost), 2)::text as value
            FROM healthcare.claims
            """
            summary_df = pd.read_sql(summary_query, conn)
            
            # Top cost drivers
            cost_drivers_query = """
            SELECT 
                diagnosis_code,
                description,
                total_cost,
                claim_count,
                avg_cost_per_claim,
                readmission_rate
            FROM healthcare.diagnosis_cost_analysis
            LIMIT 10
            """
            cost_drivers_df = pd.read_sql(cost_drivers_query, conn)
            
            # Hospital performance
            hospital_perf_query = """
            SELECT 
                hospital_name,
                state,
                total_claims,
                total_revenue,
                avg_cost_per_claim,
                readmission_rate_pct
            FROM healthcare.provider_performance
            ORDER BY total_revenue DESC
            LIMIT 15
            """
            hospital_perf_df = pd.read_sql(hospital_perf_query, conn)
            
            # Monthly trends
            monthly_trends_query = """
            SELECT 
                admission_year,
                admission_month,
                COUNT(claim_id) as claim_count,
                SUM(cost) as total_cost,
                AVG(cost) as avg_cost_per_claim,
                AVG(length_of_stay) as avg_length_of_stay
            FROM healthcare.claims
            GROUP BY admission_year, admission_month
            ORDER BY admission_year, admission_month
            """
            monthly_trends_df = pd.read_sql(monthly_trends_query, conn)
            
            # Patient demographics
            patient_demo_query = """
            SELECT 
                age_category,
                gender,
                insurance_type,
                COUNT(*) as patient_count,
                AVG(chronic_conditions) as avg_chronic_conditions
            FROM healthcare.patients
            GROUP BY age_category, gender, insurance_type
            """
            patient_demo_df = pd.read_sql(patient_demo_query, conn)
            
            # Readmission analysis
            readmission_query = """
            SELECT 
                hospital_name,
                state,
                total_claims,
                readmissions,
                readmission_rate_pct
            FROM healthcare.provider_performance
            WHERE total_claims >= 10
            ORDER BY readmission_rate_pct DESC
            LIMIT 15
            """
            readmission_df = pd.read_sql(readmission_query, conn)
            
            return {
                'summary': summary_df,
                'cost_drivers': cost_drivers_df,
                'hospital_performance': hospital_perf_df,
                'monthly_trends': monthly_trends_df,
                'patient_demographics': patient_demo_df,
                'readmission_analysis': readmission_df
            }
            
    except Exception as e:
        st.warning(f"Database connection failed: {e}")
        st.info("Falling back to sample data from files...")
        return load_data_from_files()

def create_kpi_cards(data):
    """Create KPI cards for key metrics."""
    if not data:
        return
    
    summary = data['summary']
    
    # Create columns for KPI cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        patients = summary[summary['metric'] == 'Total Patients']['value'].iloc[0]
        st.metric(
            label="Total Patients",
            value=patients,
            delta=None
        )
    
    with col2:
        claims = summary[summary['metric'] == 'Total Claims']['value'].iloc[0]
        st.metric(
            label="Total Claims",
            value=claims,
            delta=None
        )
    
    with col3:
        providers = summary[summary['metric'] == 'Total Providers']['value'].iloc[0]
        st.metric(
            label="Total Providers",
            value=providers,
            delta=None
        )
    
    with col4:
        total_cost = summary[summary['metric'] == 'Total Cost']['value'].iloc[0]
        st.metric(
            label="Total Healthcare Cost",
            value=total_cost,
            delta=None
        )

def create_cost_analysis_charts(data):
    """Create cost analysis charts."""
    if not data or data['cost_drivers'].empty:
        return
    
    st.subheader("💰 Cost Analysis")
    
    # Top 10 cost drivers
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            data['cost_drivers'].head(10),
            x='total_cost',
            y='diagnosis_code',
            orientation='h',
            title="Top 10 Cost Drivers by Diagnosis",
            labels={'total_cost': 'Total Cost ($)', 'diagnosis_code': 'Diagnosis Code'},
            color='total_cost',
            color_continuous_scale='Blues'
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.scatter(
            data['cost_drivers'].head(10),
            x='claim_count',
            y='avg_cost_per_claim',
            size='total_cost',
            hover_data=['description', 'readmission_rate'],
            title="Cost vs Volume Analysis",
            labels={'claim_count': 'Number of Claims', 'avg_cost_per_claim': 'Avg Cost per Claim ($)'}
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

def create_hospital_performance_charts(data):
    """Create hospital performance charts."""
    if not data or data['hospital_performance'].empty:
        return
    
    st.subheader("🏥 Hospital Performance")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            data['hospital_performance'].head(10),
            x='total_revenue',
            y='hospital_name',
            orientation='h',
            title="Top 10 Hospitals by Revenue",
            labels={'total_revenue': 'Total Revenue ($)', 'hospital_name': 'Hospital Name'},
            color='total_revenue',
            color_continuous_scale='Greens'
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.scatter(
            data['hospital_performance'],
            x='total_claims',
            y='readmission_rate_pct',
            size='total_revenue',
            hover_data=['hospital_name', 'state', 'avg_cost_per_claim'],
            title="Readmission Rate vs Volume",
            labels={'total_claims': 'Total Claims', 'readmission_rate_pct': 'Readmission Rate (%)'}
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

def create_trend_analysis_charts(data):
    """Create trend analysis charts."""
    if not data or data['monthly_trends'].empty:
        return
    
    st.subheader("📈 Trend Analysis")
    
    # Prepare data for trend analysis
    monthly_data = data['monthly_trends'].copy()
    monthly_data['date'] = pd.to_datetime(
        monthly_data['admission_year'].astype(str) + '-' + 
        monthly_data['admission_month'].astype(str).str.zfill(2) + '-01'
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.line(
            monthly_data,
            x='date',
            y='total_cost',
            title="Monthly Healthcare Costs",
            labels={'total_cost': 'Total Cost ($)', 'date': 'Month'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.line(
            monthly_data,
            x='date',
            y='claim_count',
            title="Monthly Claim Volume",
            labels={'claim_count': 'Number of Claims', 'date': 'Month'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

def create_patient_demographics_charts(data):
    """Create patient demographics charts."""
    if not data or data['patient_demographics'].empty:
        return
    
    st.subheader("👥 Patient Demographics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Age distribution
        age_dist = data['patient_demographics'].groupby('age_category')['patient_count'].sum().reset_index()
        fig = px.pie(
            age_dist,
            values='patient_count',
            names='age_category',
            title="Patient Distribution by Age Category"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Insurance type distribution
        insurance_dist = data['patient_demographics'].groupby('insurance_type')['patient_count'].sum().reset_index()
        fig = px.bar(
            insurance_dist,
            x='insurance_type',
            y='patient_count',
            title="Patient Distribution by Insurance Type",
            labels={'patient_count': 'Number of Patients', 'insurance_type': 'Insurance Type'}
        )
        st.plotly_chart(fig, use_container_width=True)

def create_readmission_analysis_charts(data):
    """Create readmission analysis charts."""
    if not data or data['readmission_analysis'].empty:
        return
    
    st.subheader("🔄 Readmission Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            data['readmission_analysis'].head(10),
            x='readmission_rate_pct',
            y='hospital_name',
            orientation='h',
            title="Top 10 Hospitals by Readmission Rate",
            labels={'readmission_rate_pct': 'Readmission Rate (%)', 'hospital_name': 'Hospital Name'},
            color='readmission_rate_pct',
            color_continuous_scale='Reds'
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.scatter(
            data['readmission_analysis'],
            x='total_claims',
            y='readmission_rate_pct',
            size='readmissions',
            hover_data=['hospital_name', 'state'],
            title="Readmission Rate vs Hospital Volume",
            labels={'total_claims': 'Total Claims', 'readmission_rate_pct': 'Readmission Rate (%)'}
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

def create_geographic_analysis(data):
    """Create geographic analysis charts."""
    if not data or data['hospital_performance'].empty:
        return
    
    st.subheader("🗺️ Geographic Analysis")
    
    # State-level analysis
    state_analysis = data['hospital_performance'].groupby('state').agg({
        'total_revenue': 'sum',
        'total_claims': 'sum',
        'readmission_rate_pct': 'mean'
    }).reset_index()
    
    fig = px.choropleth(
        state_analysis,
        locations='state',
        color='total_revenue',
        hover_data=['total_claims', 'readmission_rate_pct'],
        title="Healthcare Revenue by State",
        locationmode='USA-states',
        color_continuous_scale='Blues'
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)

def main():
    """Main dashboard function."""
    # Header
    st.markdown('<h1 class="main-header">🏥 Healthcare Analytics Dashboard</h1>', unsafe_allow_html=True)
    
    # Sidebar
    st.sidebar.title("Dashboard Controls")
    
    # Data refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    # Date range selector (placeholder for future enhancement)
    st.sidebar.subheader("Date Range")
    start_date = st.sidebar.date_input("Start Date", value=datetime.now() - timedelta(days=365))
    end_date = st.sidebar.date_input("End Date", value=datetime.now())
    
    # Filter options
    st.sidebar.subheader("Filters")
    selected_states = st.sidebar.multiselect(
        "Select States",
        options=['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI'],
        default=['CA', 'NY', 'TX', 'FL', 'IL']
    )
    
    # Load data
    with st.spinner("Loading healthcare data..."):
        data = load_data_from_db()
    
    if not data:
        st.error("Failed to load data. Please check your database connection.")
        return
    
    # Main dashboard content
    st.markdown("---")
    
    # KPI Cards
    create_kpi_cards(data)
    
    st.markdown("---")
    
    # Cost Analysis
    create_cost_analysis_charts(data)
    
    st.markdown("---")
    
    # Hospital Performance
    create_hospital_performance_charts(data)
    
    st.markdown("---")
    
    # Trend Analysis
    create_trend_analysis_charts(data)
    
    st.markdown("---")
    
    # Patient Demographics
    create_patient_demographics_charts(data)
    
    st.markdown("---")
    
    # Readmission Analysis
    create_readmission_analysis_charts(data)
    
    st.markdown("---")
    
    # Geographic Analysis
    create_geographic_analysis(data)
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666; padding: 2rem;'>
            <p>Healthcare Analytics Dashboard | Powered by Streamlit & PostgreSQL</p>
            <p>Last updated: {}</p>
        </div>
        """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()

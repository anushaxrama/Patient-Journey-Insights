"""
Simplified Healthcare Analytics Dashboard
Works without database connection using sample data.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime

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
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_sample_data():
    """Load sample data from parquet files."""
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
        claims_copy = claims_df.copy()
        claims_copy['admission_date_parsed'] = pd.to_datetime(claims_copy['admission_date'])
        monthly_trends = claims_copy.groupby([
            claims_copy['admission_date_parsed'].dt.year,
            claims_copy['admission_date_parsed'].dt.month
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
        st.error(f"Error loading data: {e}")
        return None

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

def main():
    """Main dashboard function."""
    # Header
    st.markdown('<h1 class="main-header">🏥 Healthcare Analytics Dashboard</h1>', unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading healthcare data..."):
        data = load_sample_data()
    
    if not data:
        st.error("Failed to load data. Please check your data files.")
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
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666; padding: 2rem;'>
            <p>Healthcare Analytics Dashboard | Powered by Streamlit & Sample Data</p>
            <p>Last updated: {}</p>
        </div>
        """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()

"""
Working Healthcare Analytics Dashboard
Fully functional dashboard that works with sample data.
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
def load_healthcare_data():
    """Load healthcare data from parquet files."""
    try:
        # Load all data files
        claims_df = pd.read_parquet('data/silver/claims_clean.parquet')
        patients_df = pd.read_parquet('data/silver/patients_clean.parquet')
        providers_df = pd.read_parquet('data/silver/providers_clean.parquet')
        prescriptions_df = pd.read_parquet('data/silver/prescriptions_clean.parquet')
        
        return {
            'claims': claims_df,
            'patients': patients_df,
            'providers': providers_df,
            'prescriptions': prescriptions_df
        }
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

def create_summary_metrics(data):
    """Create summary KPI cards."""
    claims_df = data['claims']
    patients_df = data['patients']
    providers_df = data['providers']
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Patients",
            value=f"{len(patients_df):,}",
            delta=None
        )
    
    with col2:
        st.metric(
            label="Total Claims",
            value=f"{len(claims_df):,}",
            delta=None
        )
    
    with col3:
        st.metric(
            label="Total Providers",
            value=f"{len(providers_df):,}",
            delta=None
        )
    
    with col4:
        total_cost = claims_df['cost'].sum()
        st.metric(
            label="Total Healthcare Cost",
            value=f"${total_cost:,.0f}",
            delta=None
        )

def create_cost_analysis(data):
    """Create cost analysis charts."""
    claims_df = data['claims']
    
    st.subheader("💰 Cost Analysis")
    
    # Top 10 cost drivers
    cost_by_diagnosis = claims_df.groupby('diagnosis_code')['cost'].agg(['sum', 'mean', 'count']).round(2)
    cost_by_diagnosis.columns = ['total_cost', 'avg_cost', 'claim_count']
    cost_by_diagnosis = cost_by_diagnosis.sort_values('total_cost', ascending=False).head(10)
    cost_by_diagnosis = cost_by_diagnosis.reset_index()
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            cost_by_diagnosis,
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
            cost_by_diagnosis,
            x='claim_count',
            y='avg_cost',
            size='total_cost',
            title="Cost vs Volume Analysis",
            labels={'claim_count': 'Number of Claims', 'avg_cost': 'Avg Cost per Claim ($)'}
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

def create_hospital_performance(data):
    """Create hospital performance charts."""
    claims_df = data['claims']
    providers_df = data['providers']
    
    st.subheader("🏥 Hospital Performance")
    
    # Hospital performance analysis
    hospital_perf = claims_df.groupby('provider_id').agg({
        'cost': ['sum', 'mean'],
        'claim_id': 'count',
        'readmission_flag': 'mean'
    }).round(2)
    hospital_perf.columns = ['total_revenue', 'avg_cost_per_claim', 'total_claims', 'readmission_rate_pct']
    hospital_perf = hospital_perf.reset_index()
    hospital_perf = hospital_perf.merge(providers_df[['provider_id', 'hospital_name', 'state']], on='provider_id')
    hospital_perf = hospital_perf.sort_values('total_revenue', ascending=False).head(15)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            hospital_perf.head(10),
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
            hospital_perf,
            x='total_claims',
            y='readmission_rate_pct',
            size='total_revenue',
            hover_data=['hospital_name', 'state', 'avg_cost_per_claim'],
            title="Readmission Rate vs Volume",
            labels={'total_claims': 'Total Claims', 'readmission_rate_pct': 'Readmission Rate (%)'}
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

def create_patient_demographics(data):
    """Create patient demographics charts."""
    patients_df = data['patients']
    
    st.subheader("👥 Patient Demographics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Age distribution
        age_dist = patients_df['age_category'].value_counts()
        fig = px.pie(
            values=age_dist.values,
            names=age_dist.index,
            title="Patient Distribution by Age Category"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Gender distribution
        gender_dist = patients_df['gender'].value_counts()
        fig = px.bar(
            x=gender_dist.index,
            y=gender_dist.values,
            title="Patient Distribution by Gender",
            labels={'x': 'Gender', 'y': 'Number of Patients'}
        )
        st.plotly_chart(fig, use_container_width=True)

def create_readmission_analysis(data):
    """Create readmission analysis charts."""
    claims_df = data['claims']
    providers_df = data['providers']
    
    st.subheader("🔄 Readmission Analysis")
    
    # Readmission analysis
    hospital_perf = claims_df.groupby('provider_id').agg({
        'cost': ['sum', 'mean'],
        'claim_id': 'count',
        'readmission_flag': 'mean'
    }).round(2)
    hospital_perf.columns = ['total_revenue', 'avg_cost_per_claim', 'total_claims', 'readmission_rate_pct']
    hospital_perf = hospital_perf.reset_index()
    hospital_perf = hospital_perf.merge(providers_df[['provider_id', 'hospital_name', 'state']], on='provider_id')
    hospital_perf = hospital_perf[hospital_perf['total_claims'] >= 10]
    hospital_perf = hospital_perf.sort_values('readmission_rate_pct', ascending=False).head(15)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            hospital_perf.head(10),
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
        # Overall readmission statistics
        overall_readmission_rate = claims_df['readmission_flag'].mean() * 100
        total_readmissions = claims_df['readmission_flag'].sum()
        
        st.metric("Overall Readmission Rate", f"{overall_readmission_rate:.1f}%")
        st.metric("Total Readmissions", f"{total_readmissions:,}")
        st.metric("Average Cost per Claim", f"${claims_df['cost'].mean():,.2f}")
        st.metric("Average Length of Stay", f"{claims_df['length_of_stay'].mean():.1f} days")

def main():
    """Main dashboard function."""
    # Header
    st.markdown('<h1 class="main-header">🏥 Healthcare Analytics Dashboard</h1>', unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading healthcare data..."):
        data = load_healthcare_data()
    
    if not data:
        st.error("Failed to load data. Please check your data files.")
        return
    
    # Main dashboard content
    st.markdown("---")
    
    # Summary Metrics
    create_summary_metrics(data)
    
    st.markdown("---")
    
    # Cost Analysis
    create_cost_analysis(data)
    
    st.markdown("---")
    
    # Hospital Performance
    create_hospital_performance(data)
    
    st.markdown("---")
    
    # Patient Demographics
    create_patient_demographics(data)
    
    st.markdown("---")
    
    # Readmission Analysis
    create_readmission_analysis(data)
    
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

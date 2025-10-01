"""
Simplified Healthcare AI Chatbot
Works without database connection using sample data.
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Healthcare AI Chatbot",
    page_icon="🤖",
    layout="wide"
)

# Header
st.markdown("""
<div style='text-align: center; padding: 2rem; background: linear-gradient(90deg, #1f77b4, #ff7f0e); color: white; border-radius: 10px; margin-bottom: 2rem;'>
    <h1>🤖 Healthcare AI Chatbot</h1>
    <p>Ask questions about your healthcare data in natural language</p>
</div>
""", unsafe_allow_html=True)

@st.cache_data
def load_sample_data():
    """Load sample data for analysis."""
    try:
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

def analyze_query(query, data):
    """Analyze user query and provide response."""
    query_lower = query.lower()
    
    # Cost analysis queries
    if any(word in query_lower for word in ['cost', 'expensive', 'expensive', 'revenue', 'money']):
        if 'top' in query_lower and 'diagnosis' in query_lower:
            cost_by_diagnosis = data['claims'].groupby('diagnosis_code')['cost'].sum().sort_values(ascending=False).head(10)
            response = f"**Top 10 Most Expensive Diagnosis Codes:**\n\n"
            for i, (diagnosis, cost) in enumerate(cost_by_diagnosis.items(), 1):
                response += f"{i}. {diagnosis}: ${cost:,.2f}\n"
            return response
        
        elif 'total' in query_lower:
            total_cost = data['claims']['cost'].sum()
            avg_cost = data['claims']['cost'].mean()
            response = f"**Healthcare Cost Summary:**\n\n"
            response += f"• Total Cost: ${total_cost:,.2f}\n"
            response += f"• Average Cost per Claim: ${avg_cost:,.2f}\n"
            response += f"• Total Claims: {len(data['claims']):,}\n"
            return response
    
    # Readmission analysis queries
    elif any(word in query_lower for word in ['readmission', 'readmit', 'return']):
        readmission_rate = data['claims']['readmission_flag'].mean() * 100
        total_readmissions = data['claims']['readmission_flag'].sum()
        
        response = f"**Readmission Analysis:**\n\n"
        response += f"• Overall Readmission Rate: {readmission_rate:.1f}%\n"
        response += f"• Total Readmissions: {total_readmissions:,}\n"
        response += f"• Total Claims: {len(data['claims']):,}\n"
        
        # Top readmission diagnoses
        readmission_by_diagnosis = data['claims'].groupby('diagnosis_code')['readmission_flag'].mean().sort_values(ascending=False).head(5)
        response += f"\n**Top 5 Diagnosis Codes by Readmission Rate:**\n"
        for diagnosis, rate in readmission_by_diagnosis.items():
            response += f"• {diagnosis}: {rate*100:.1f}%\n"
        
        return response
    
    # Patient demographics queries
    elif any(word in query_lower for word in ['patient', 'demographic', 'age', 'gender']):
        avg_age = data['patients']['age'].mean()
        gender_dist = data['patients']['gender'].value_counts()
        age_categories = data['patients']['age_category'].value_counts()
        
        response = f"**Patient Demographics:**\n\n"
        response += f"• Total Patients: {len(data['patients']):,}\n"
        response += f"• Average Age: {avg_age:.1f} years\n"
        response += f"• Gender Distribution:\n"
        for gender, count in gender_dist.items():
            response += f"  - {gender}: {count:,} ({count/len(data['patients'])*100:.1f}%)\n"
        response += f"• Age Categories:\n"
        for category, count in age_categories.items():
            response += f"  - {category}: {count:,} ({count/len(data['patients'])*100:.1f}%)\n"
        
        return response
    
    # Hospital performance queries
    elif any(word in query_lower for word in ['hospital', 'provider', 'facility']):
        hospital_perf = data['claims'].groupby('provider_id').agg({
            'cost': ['sum', 'mean'],
            'claim_id': 'count',
            'readmission_flag': 'mean'
        }).round(2)
        hospital_perf.columns = ['total_revenue', 'avg_cost_per_claim', 'total_claims', 'readmission_rate_pct']
        hospital_perf = hospital_perf.reset_index()
        hospital_perf = hospital_perf.merge(data['providers'][['provider_id', 'hospital_name', 'state']], on='provider_id')
        hospital_perf = hospital_perf.sort_values('total_revenue', ascending=False).head(10)
        
        response = f"**Top 10 Hospitals by Revenue:**\n\n"
        for i, row in hospital_perf.iterrows():
            response += f"{i+1}. {row['hospital_name']} ({row['state']}): ${row['total_revenue']:,.2f}\n"
        
        return response
    
    # Medication queries
    elif any(word in query_lower for word in ['medication', 'prescription', 'drug', 'adherence']):
        if 'adherence' in query_lower:
            # Calculate adherence rate
            prescriptions_df = data['prescriptions'].copy()
            prescriptions_df['adherence_rate'] = prescriptions_df['days_supplied'] / prescriptions_df['days_prescribed']
            prescriptions_df['adherence_rate'] = prescriptions_df['adherence_rate'].clip(0, 1)
            avg_adherence = prescriptions_df['adherence_rate'].mean() * 100
            
            response = f"**Medication Adherence Analysis:**\n\n"
            response += f"• Average Adherence Rate: {avg_adherence:.1f}%\n"
            response += f"• Total Prescriptions: {len(prescriptions_df):,}\n"
            
            # Top medications by adherence
            med_adherence = prescriptions_df.groupby('medication_name')['adherence_rate'].mean().sort_values(ascending=False).head(5)
            response += f"\n**Top 5 Medications by Adherence:**\n"
            for med, rate in med_adherence.items():
                response += f"• {med}: {rate*100:.1f}%\n"
            
            return response
        else:
            top_meds = data['prescriptions']['medication_name'].value_counts().head(10)
            response = f"**Top 10 Most Prescribed Medications:**\n\n"
            for i, (med, count) in enumerate(top_meds.items(), 1):
                response += f"{i}. {med}: {count:,} prescriptions\n"
            return response
    
    # General statistics
    elif any(word in query_lower for word in ['summary', 'overview', 'statistics', 'stats']):
        response = f"**Healthcare Data Summary:**\n\n"
        response += f"• **Patients**: {len(data['patients']):,}\n"
        response += f"• **Claims**: {len(data['claims']):,}\n"
        response += f"• **Providers**: {len(data['providers']):,}\n"
        response += f"• **Prescriptions**: {len(data['prescriptions']):,}\n"
        response += f"• **Total Cost**: ${data['claims']['cost'].sum():,.2f}\n"
        response += f"• **Average Cost per Claim**: ${data['claims']['cost'].mean():,.2f}\n"
        response += f"• **Readmission Rate**: {data['claims']['readmission_flag'].mean()*100:.1f}%\n"
        response += f"• **Average Length of Stay**: {data['claims']['length_of_stay'].mean():.1f} days\n"
        
        return response
    
    # Default response
    else:
        return f"I can help you analyze healthcare data! Try asking about:\n\n• **Costs**: 'What are the top cost drivers?'\n• **Readmissions**: 'Show me readmission rates by diagnosis'\n• **Patients**: 'What are the patient demographics?'\n• **Hospitals**: 'Which hospitals have the highest revenue?'\n• **Medications**: 'What are the medication adherence rates?'\n• **Summary**: 'Give me a data overview'"

def main():
    """Main chatbot function."""
    # Load data
    with st.spinner("Loading healthcare data..."):
        data = load_sample_data()
    
    if not data:
        st.error("Failed to load data. Please check your data files.")
        return
    
    # Sidebar with sample questions
    with st.sidebar:
        st.header("💡 Sample Questions")
        st.markdown("Click on any question below to try it out:")
        
        sample_questions = [
            "What are the top 5 most expensive diagnosis codes?",
            "What is the overall readmission rate?",
            "Show me patient demographics by age and gender",
            "Which hospitals have the highest revenue?",
            "What are the medication adherence rates?",
            "Give me a summary of the healthcare data"
        ]
        
        for i, question in enumerate(sample_questions):
            if st.button(f"{i+1}. {question}", key=f"sample_{i}"):
                st.session_state.user_input = question
    
    # Main chat interface
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Chat input
        user_input = st.text_input(
            "Ask a question about your healthcare data:",
            value=st.session_state.get('user_input', ''),
            placeholder="e.g., What are the top 5 cost drivers by diagnosis?",
            key="chat_input"
        )
    
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)  # Spacing
        if st.button("🚀 Ask", type="primary", use_container_width=True):
            if user_input:
                st.session_state.user_input = user_input
    
    # Process query
    if st.session_state.get('user_input'):
        query = st.session_state.user_input
        
        # Clear the input
        st.session_state.user_input = ""
        
        # Process the query
        with st.spinner("🤔 Analyzing your question..."):
            response = analyze_query(query, data)
        
        # Display results
        st.markdown("### 📊 Analysis Results")
        st.markdown(response)
        
        # Add some styling
        st.markdown("---")
        st.markdown(f"**Query:** {query}")
        st.markdown(f"**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 1rem;'>
        <p>Healthcare AI Chatbot | Powered by Sample Data</p>
        <p>Ask questions about costs, readmissions, patient demographics, and more!</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()

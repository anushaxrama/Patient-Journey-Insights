"""
Healthcare AI Chatbot
LangChain-powered chatbot for natural language queries on healthcare data.
"""

import os
import sys
import streamlit as st
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd
from sqlalchemy import create_engine, text

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils import get_database_connection

# LangChain imports
try:
    from langchain.llms import OpenAI
    from langchain.chat_models import ChatOpenAI
    from langchain.schema import HumanMessage, SystemMessage
    from langchain.agents import create_sql_agent, AgentExecutor
    from langchain.agents.agent_toolkits import SQLDatabaseToolkit
    from langchain.sql_database import SQLDatabase
    from langchain.agents.agent_types import AgentType
    from langchain.memory import ConversationBufferMemory
    from langchain.prompts import PromptTemplate
    from langchain.chains import LLMChain
except ImportError:
    st.error("LangChain not installed. Please install with: pip install langchain openai")
    st.stop()

class HealthcareChatbot:
    """AI-powered chatbot for healthcare data queries."""
    
    def __init__(self):
        self.llm = None
        self.db = None
        self.agent = None
        self.memory = None
        self.setup_llm()
        self.setup_database()
        self.setup_agent()
    
    def setup_llm(self):
        """Setup the language model."""
        try:
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                st.error("OpenAI API key not found. Please set OPENAI_API_KEY environment variable.")
                return
            
            self.llm = ChatOpenAI(
                openai_api_key=api_key,
                model_name="gpt-3.5-turbo",
                temperature=0.1,
                max_tokens=1000
            )
            st.success("✅ Language model initialized successfully")
            
        except Exception as e:
            st.error(f"Failed to initialize language model: {e}")
    
    def setup_database(self):
        """Setup database connection."""
        try:
            connection_string = get_database_connection()
            self.db = SQLDatabase.from_uri(connection_string)
            st.success("✅ Database connection established")
            
        except Exception as e:
            st.warning(f"Database connection failed: {e}")
            st.info("Chatbot will work with sample data instead")
            self.db = None
    
    def setup_agent(self):
        """Setup the SQL agent."""
        try:
            if not self.llm or not self.db:
                return
            
            toolkit = SQLDatabaseToolkit(db=self.db, llm=self.llm)
            self.agent = create_sql_agent(
                llm=self.llm,
                toolkit=toolkit,
                agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
                verbose=True,
                handle_parsing_errors=True
            )
            
            # Setup memory for conversation
            self.memory = ConversationBufferMemory(
                memory_key="chat_history",
                return_messages=True
            )
            
            st.success("✅ AI Agent initialized successfully")
            
        except Exception as e:
            st.error(f"Failed to setup AI agent: {e}")
    
    def get_system_prompt(self) -> str:
        """Get the system prompt for the healthcare chatbot."""
        return """
        You are a healthcare data analyst AI assistant. You help users analyze healthcare data by:
        
        1. Understanding natural language questions about healthcare metrics
        2. Converting questions to SQL queries against the healthcare database
        3. Executing queries and presenting results in a clear, actionable format
        4. Providing insights and recommendations based on the data
        
        Database Schema:
        - patients: Patient demographics and clinical information
        - providers: Healthcare providers and facilities
        - claims: Healthcare claims and billing information
        - prescriptions: Medication prescriptions and adherence data
        - medications: Medication reference data
        - diagnosis_codes: ICD-10 diagnosis code reference data
        
        Key Metrics Available:
        - Cost analysis by diagnosis, provider, patient demographics
        - Readmission rates by hospital and diagnosis
        - Medication adherence rates
        - Patient journey analysis
        - Provider performance metrics
        
        Always provide:
        - Clear explanations of what the data shows
        - Business insights and recommendations
        - Visual suggestions when appropriate
        - Context about data limitations
        
        Be helpful, accurate, and professional in your responses.
        """
    
    def process_query(self, user_query: str) -> Dict[str, Any]:
        """Process a user query and return results."""
        try:
            if not self.agent:
                return {
                    "success": False,
                    "error": "AI agent not initialized",
                    "response": "Please check your configuration and try again."
                }
            
            # Add context to the query
            contextual_query = f"""
            Healthcare Data Query: {user_query}
            
            Please analyze this query and provide:
            1. The SQL query used
            2. The results
            3. Key insights
            4. Business recommendations
            """
            
            # Execute the query using the agent
            response = self.agent.run(contextual_query)
            
            return {
                "success": True,
                "response": response,
                "query": user_query,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "response": f"I encountered an error processing your query: {str(e)}. Please try rephrasing your question."
            }
    
    def get_sample_questions(self) -> List[str]:
        """Get sample questions users can ask."""
        return [
            "What are the top 10 most expensive diagnosis codes?",
            "Which hospitals have the highest readmission rates?",
            "What is the average cost per claim by patient age group?",
            "Show me medication adherence rates by drug category",
            "Which states have the highest healthcare costs?",
            "What is the trend in healthcare costs over time?",
            "Which patients are at highest risk for readmission?",
            "What are the most common diagnoses for patients over 65?",
            "How does insurance type affect healthcare costs?",
            "What is the average length of stay by hospital?",
            "Which medications have the lowest adherence rates?",
            "Show me cost analysis by patient gender and age",
            "What are the seasonal patterns in healthcare utilization?",
            "Which providers have the best cost efficiency?",
            "What is the correlation between chronic conditions and costs?"
        ]

def create_chatbot_interface():
    """Create the Streamlit interface for the chatbot."""
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
    
    # Initialize chatbot
    if 'chatbot' not in st.session_state:
        with st.spinner("Initializing AI chatbot..."):
            st.session_state.chatbot = HealthcareChatbot()
    
    # Sidebar with sample questions
    with st.sidebar:
        st.header("💡 Sample Questions")
        st.markdown("Click on any question below to try it out:")
        
        chatbot = st.session_state.chatbot
        sample_questions = chatbot.get_sample_questions()
        
        for i, question in enumerate(sample_questions):
            if st.button(f"{i+1}. {question}", key=f"sample_{i}"):
                st.session_state.user_input = question
        
        st.markdown("---")
        st.markdown("### 🎯 Tips for Better Results")
        st.markdown("""
        - Be specific about time periods
        - Mention specific metrics you want
        - Ask for comparisons or trends
        - Request visualizations when helpful
        """)
    
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
            result = st.session_state.chatbot.process_query(query)
        
        # Display results
        if result["success"]:
            st.markdown("### 📊 Analysis Results")
            
            # Display the response
            st.markdown(result["response"])
            
            # Add some styling
            st.markdown("---")
            st.markdown(f"**Query:** {query}")
            st.markdown(f"**Timestamp:** {result['timestamp']}")
            
        else:
            st.error(f"❌ Error: {result['error']}")
            st.markdown(f"**Response:** {result['response']}")
    
    # Chat history (if implemented)
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    
    # Display chat history
    if st.session_state.chat_history:
        st.markdown("### 💬 Chat History")
        for i, chat in enumerate(reversed(st.session_state.chat_history[-5:])):  # Show last 5
            with st.expander(f"Query {len(st.session_state.chat_history) - i}: {chat['query'][:50]}..."):
                st.markdown(f"**Question:** {chat['query']}")
                st.markdown(f"**Answer:** {chat['response']}")
                st.markdown(f"**Time:** {chat['timestamp']}")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 1rem;'>
        <p>Healthcare AI Chatbot | Powered by LangChain & OpenAI</p>
        <p>Ask questions about costs, readmissions, patient demographics, and more!</p>
    </div>
    """, unsafe_allow_html=True)

def main():
    """Main function to run the chatbot."""
    create_chatbot_interface()

if __name__ == "__main__":
    main()

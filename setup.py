#!/usr/bin/env python3
"""
Healthcare ETL Pipeline Setup Script
Automated setup and configuration for the healthcare analytics platform.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

def run_command(command, description):
    """Run a command and handle errors."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        print(f"Error output: {e.stderr}")
        return False

def check_python_version():
    """Check if Python version is compatible."""
    print("🐍 Checking Python version...")
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        return False
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor} detected")
    return True

def check_dependencies():
    """Check if required dependencies are available."""
    print("📦 Checking dependencies...")
    
    required_packages = [
        'pandas', 'numpy', 'sqlalchemy', 'psycopg2-binary',
        'streamlit', 'plotly', 'langchain', 'openai'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"❌ Missing packages: {', '.join(missing_packages)}")
        print("Run: pip install -r requirements.txt")
        return False
    
    print("✅ All required packages are available")
    return True

def create_directories():
    """Create necessary directories."""
    print("📁 Creating directories...")
    
    directories = [
        'data/bronze',
        'data/silver', 
        'data/gold',
        'logs',
        'notebooks',
        'dashboards'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"  ✅ Created {directory}/")
    
    return True

def setup_environment():
    """Setup environment variables."""
    print("🔧 Setting up environment...")
    
    env_file = Path('.env')
    if not env_file.exists():
        env_example = Path('env.example')
        if env_example.exists():
            shutil.copy(env_example, env_file)
            print("✅ Created .env file from template")
            print("⚠️  Please update .env with your actual configuration")
        else:
            print("❌ env.example file not found")
            return False
    else:
        print("✅ .env file already exists")
    
    return True

def setup_database():
    """Setup PostgreSQL database."""
    print("🗄️  Setting up database...")
    
    # Check if PostgreSQL is available
    if not run_command("which psql", "Checking PostgreSQL installation"):
        print("❌ PostgreSQL not found. Please install PostgreSQL first.")
        print("   macOS: brew install postgresql")
        print("   Ubuntu: sudo apt-get install postgresql postgresql-contrib")
        return False
    
    # Create database (this will fail if database already exists, which is fine)
    run_command(
        "createdb healthcare_analytics",
        "Creating healthcare_analytics database"
    )
    
    print("✅ Database setup completed")
    return True

def run_etl_pipeline():
    """Run the ETL pipeline."""
    print("🚀 Running ETL pipeline...")
    
    # Add src to Python path
    sys.path.insert(0, 'src')
    
    try:
        # Import and run ETL modules
        from extract import HealthcareDataExtractor
        from transform import HealthcareDataTransformer
        from load import HealthcareDataLoader
        
        print("  📥 Extracting data...")
        extractor = HealthcareDataExtractor()
        extractor.extract_all_data()
        
        print("  🔄 Transforming data...")
        transformer = HealthcareDataTransformer()
        transformer.transform_all_data()
        
        print("  📤 Loading data...")
        loader = HealthcareDataLoader()
        success = loader.load_all_data()
        
        if success:
            print("✅ ETL pipeline completed successfully")
            return True
        else:
            print("❌ ETL pipeline failed")
            return False
            
    except Exception as e:
        print(f"❌ ETL pipeline error: {e}")
        return False

def test_dashboard():
    """Test the Streamlit dashboard."""
    print("🎨 Testing dashboard...")
    
    # Check if Streamlit can import the dashboard
    try:
        import streamlit as st
        sys.path.insert(0, 'streamlit_app')
        import dashboard
        print("✅ Dashboard imports successfully")
        return True
    except Exception as e:
        print(f"❌ Dashboard test failed: {e}")
        return False

def test_chatbot():
    """Test the AI chatbot."""
    print("🤖 Testing chatbot...")
    
    # Check if chatbot can be imported
    try:
        sys.path.insert(0, 'chatbot')
        import healthcare_chatbot
        print("✅ Chatbot imports successfully")
        return True
    except Exception as e:
        print(f"❌ Chatbot test failed: {e}")
        return False

def print_next_steps():
    """Print next steps for the user."""
    print("\n" + "="*60)
    print("🎉 Healthcare ETL Pipeline Setup Complete!")
    print("="*60)
    
    print("\n📋 Next Steps:")
    print("1. Update .env file with your database credentials")
    print("2. Set your OpenAI API key in .env for the chatbot")
    print("3. Run the ETL pipeline: python src/extract.py")
    print("4. Start the dashboard: streamlit run streamlit_app/dashboard.py")
    print("5. Start the chatbot: streamlit run chatbot/healthcare_chatbot.py")
    print("6. Start Airflow: airflow webserver --port 8080")
    
    print("\n🌐 Access URLs:")
    print("  • Dashboard: http://localhost:8501")
    print("  • Chatbot: http://localhost:8502")
    print("  • Airflow: http://localhost:8080")
    
    print("\n📚 Documentation:")
    print("  • README.md - Project overview")
    print("  • docs/architecture.md - System architecture")
    print("  • sql/queries.sql - Sample analytics queries")
    
    print("\n🔧 Configuration:")
    print("  • Database: PostgreSQL on localhost:5432")
    print("  • Data: ./data/ directory")
    print("  • Logs: ./logs/ directory")
    
    print("\n💡 Tips:")
    print("  • Use Docker: docker-compose up -d")
    print("  • Check logs: tail -f logs/etl_pipeline.log")
    print("  • Run tests: python -m pytest tests/")

def main():
    """Main setup function."""
    print("🏥 Healthcare ETL Pipeline Setup")
    print("="*40)
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Create directories
    if not create_directories():
        sys.exit(1)
    
    # Setup environment
    if not setup_environment():
        sys.exit(1)
    
    # Check dependencies
    if not check_dependencies():
        print("\n⚠️  Some dependencies are missing. Please install them first:")
        print("   pip install -r requirements.txt")
        sys.exit(1)
    
    # Setup database
    if not setup_database():
        print("\n⚠️  Database setup failed. Please check PostgreSQL installation.")
        print("   You can still run the pipeline with sample data.")
    
    # Test components
    test_dashboard()
    test_chatbot()
    
    # Print next steps
    print_next_steps()

if __name__ == "__main__":
    main()

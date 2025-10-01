#!/usr/bin/env python3
"""
Quick start script for the Healthcare Analytics Dashboard
"""

import subprocess
import sys
import os
from pathlib import Path

def main():
    """Run the Streamlit dashboard."""
    print("🏥 Starting Healthcare Analytics Dashboard...")
    
    # Check if streamlit is installed
    try:
        import streamlit
    except ImportError:
        print("❌ Streamlit not found. Installing...")
        subprocess.run([sys.executable, "-m", "pip", "install", "streamlit"])
    
    # Set up environment
    os.environ["PYTHONPATH"] = str(Path(__file__).parent / "src")
    
    # Run the dashboard
    dashboard_path = Path(__file__).parent / "streamlit_app" / "dashboard.py"
    
    if not dashboard_path.exists():
        print(f"❌ Dashboard file not found: {dashboard_path}")
        sys.exit(1)
    
    print("🚀 Launching dashboard at http://localhost:8501")
    print("Press Ctrl+C to stop the dashboard")
    
    try:
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            str(dashboard_path),
            "--server.port=8501",
            "--server.address=localhost"
        ])
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped")

if __name__ == "__main__":
    main()

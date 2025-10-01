#!/usr/bin/env python3
"""
Quick start script for the Healthcare AI Chatbot
"""

import subprocess
import sys
import os
from pathlib import Path

def main():
    """Run the AI chatbot."""
    print("🤖 Starting Healthcare AI Chatbot...")
    
    # Check if streamlit is installed
    try:
        import streamlit
    except ImportError:
        print("❌ Streamlit not found. Installing...")
        subprocess.run([sys.executable, "-m", "pip", "install", "streamlit"])
    
    # Check if OpenAI API key is set
    if not os.getenv("OPENAI_API_KEY"):
        print("⚠️  OPENAI_API_KEY not set. Please set it in your environment or .env file")
        print("   You can get an API key from: https://platform.openai.com/api-keys")
    
    # Set up environment
    os.environ["PYTHONPATH"] = str(Path(__file__).parent / "src")
    
    # Run the chatbot
    chatbot_path = Path(__file__).parent / "chatbot" / "healthcare_chatbot.py"
    
    if not chatbot_path.exists():
        print(f"❌ Chatbot file not found: {chatbot_path}")
        sys.exit(1)
    
    print("🚀 Launching chatbot at http://localhost:8502")
    print("Press Ctrl+C to stop the chatbot")
    
    try:
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            str(chatbot_path),
            "--server.port=8502",
            "--server.address=localhost"
        ])
    except KeyboardInterrupt:
        print("\n👋 Chatbot stopped")

if __name__ == "__main__":
    main()

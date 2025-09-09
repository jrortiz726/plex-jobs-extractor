#!/bin/bash
# Quick start script for local development

echo "======================================"
echo "Plex-CDF Extractor Quick Start"
echo "======================================"

# Check if .env exists
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found!"
    echo "Please copy .env.example to .env and configure it."
    exit 1
fi

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: Python 3 is not installed!"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install/upgrade dependencies
echo "Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Test connections first
echo ""
echo "Testing connections..."
python test_connections.py

if [ $? -ne 0 ]; then
    echo ""
    echo "❌ Connection test failed! Please check your configuration."
    exit 1
fi

echo ""
echo "✅ Connection test passed!"
echo ""
echo "Select operation mode:"
echo "1) Test mode - Run each extractor once"
echo "2) Continuous mode - Run all extractors on schedule"
echo "3) Single run - Run all extractors once and exit"
echo "4) Specific extractor - Choose which to run"
echo "5) Exit"
echo ""
read -p "Enter choice [1-5]: " choice

case $choice in
    1)
        echo "Running in test mode..."
        python orchestrator.py --mode test
        ;;
    2)
        echo "Starting continuous extraction..."
        echo "Press Ctrl+C to stop"
        python orchestrator.py --mode continuous
        ;;
    3)
        echo "Running all extractors once..."
        python orchestrator.py --mode once
        ;;
    4)
        echo "Available extractors:"
        echo "  - master_data"
        echo "  - jobs"
        echo "  - production"
        echo "  - inventory"
        read -p "Enter extractor name(s) separated by space: " extractors
        python orchestrator.py --mode once --extractors $extractors
        ;;
    5)
        echo "Exiting..."
        exit 0
        ;;
    *)
        echo "Invalid choice!"
        exit 1
        ;;
esac

echo ""
echo "Done!"
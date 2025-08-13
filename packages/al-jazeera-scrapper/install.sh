#!/bin/bash
# Simple installation script for Al Jazeera scraper

# Resolve directory of this script to work from anywhere
script_dir="$(cd "$(dirname "$0")" && pwd)"

echo "🚀 Installing Al Jazeera Scraper dependencies..."

# Check if pip3 is available
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 not found. Please install Python 3 and pip first."
    exit 1
fi

# Install requirements into local venv (handles PEP 668)
echo "📦 Creating virtual environment and installing Python packages..."
python3 -m venv "$script_dir/.venv"
source "$script_dir/.venv/bin/activate"
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade -r "$script_dir/requirements.txt"

# Check installation
echo "✅ Testing installation..."
python3 -c "import aiohttp, httpx, xmltodict, bs4, lxml, dotenv, supabase; print('All dependencies installed successfully!')"

# Setup settings file if it doesn't exist
if [ ! -f "$script_dir/settings.json" ]; then
    echo "📄 Creating settings.json from template..."
    # cp "$script_dir/settings.example.json" "$script_dir/settings.json"
    echo "⚠️  Please edit settings.json to add your proxy configuration"
fi

echo "🎉 Installation complete!"
echo ""
echo "Next steps:"
echo "  1. Edit settings.json with your proxy details"
echo "  2. python3 scraper.py        # Scrape all sitemaps"
echo "  3. python3 scraper.py 5      # Limit to first 5 sitemaps"
echo "  4. python3 test_scraper.py   # Run basic tests"
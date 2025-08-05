#!/bin/bash
# Simple installation script for Al Jazeera scraper

echo "🚀 Installing Al Jazeera Scraper dependencies..."

# Check if pip3 is available
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 not found. Please install Python 3 and pip first."
    exit 1
fi

# Install requirements
echo "📦 Installing Python packages..."
pip3 install -r requirements.txt

# Check installation
echo "✅ Testing installation..."
python3 -c "import aiohttp, xmltodict, bs4; print('All dependencies installed successfully!')"

# Setup settings file if it doesn't exist
if [ ! -f "settings.json" ]; then
    echo "📄 Creating settings.json from template..."
    cp settings.example.json settings.json
    echo "⚠️  Please edit settings.json to add your proxy configuration"
fi

echo "🎉 Installation complete!"
echo ""
echo "Next steps:"
echo "  1. Edit settings.json with your proxy details"
echo "  2. python3 scraper.py        # Scrape all sitemaps"
echo "  3. python3 scraper.py 5      # Limit to first 5 sitemaps"
echo "  4. python3 test_scraper.py   # Run basic tests"
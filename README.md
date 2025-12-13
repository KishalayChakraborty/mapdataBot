# Geolocation Data Scraper

This project scrapes address and geolocation data (latitude, longitude, address details, phone numbers, etc.) from Google Maps and other websites.

## Features

- Scrape public address data from Google Maps and other sources
- Extract latitude, longitude, address, city, state, district, phone number, etc.
- Store results in CSV files
- Uses Selenium and Playwright for dynamic sites
- BeautifulSoup for HTML parsing

## Alternatives

- **Selenium**: Automates browser for dynamic content
- **Playwright**: Modern browser automation
- **Google Maps API**: Official, requires API key
- **Other APIs**: OpenStreetMap, MapQuest, Here Maps

## Setup

1. Install Python 3.8+
2. Install dependencies: `pip install -r requirements.txt`
3. Run example script: `python src/scrape_googlemaps.py`

## Legal Notice

Scraping Google Maps may violate their terms of service. Use APIs where possible and respect robots.txt and site policies.

## Folder Structure

- `src/` - Python scripts
- `data/` - Output data
- `.github/` - Project instructions

## Usage

See `src/scrape_googlemaps.py` for a template script.

def geocode_photon(address):
    try:
        import requests
        url = "https://photon.komoot.io/api/"
        params = {"q": address, "limit": 1}
        r = requests.get(url, params=params)
        data = r.json()
        if data.get('features'):
            coords = data['features'][0]['geometry']['coordinates']
            return str(coords[1]), str(coords[0])  # lat, lon
    except Exception as e:
        print(f"Could not geocode with Photon: {e}")
    return '', ''
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from openlocationcode import openlocationcode as olc
import requests
# --- CONFIG ---
GOOGLE_MAPS_URL = "https://www.google.com/maps"


def extract_place_details(driver):
    details = {}
    # Name
    try:
        details['name'] = driver.find_element(By.CSS_SELECTOR, '.DUwDvf').text
    except:
        details['name'] = ''
    # Address
    try:
        details['address'] = driver.find_element(By.CSS_SELECTOR, 'button[data-item-id="address"] .Io6YTe').text
    except:
        details['address'] = ''
    # Phone
    try:
        details['phone'] = driver.find_element(By.CSS_SELECTOR, 'button[data-item-id^="phone"] .Io6YTe').text
    except:
        details['phone'] = ''
    # Website
    try:
        details['website'] = driver.find_element(By.CSS_SELECTOR, 'a[data-item-id="authority"] .Io6YTe').text
    except:
        details['website'] = ''
    # Plus code
    try:
        details['plus_code'] = driver.find_element(By.CSS_SELECTOR, 'button[data-item-id="oloc"] .Io6YTe').text
    except:
        details['plus_code'] = ''
    # If plus code is empty, try to extract from address
    if not details['plus_code'] and details['address']:
        import re
        match = re.search(r'\b[23456789CFGHJMPQRVWX]{4,}\+[23456789CFGHJMPQRVWX]{2,}\b', details['address'])
        if match:
            details['plus_code'] = match.group(0)
    # If lat/lon are empty and plus code is available, decode using openlocationcode
    if details.get('plus_code') :#and (not details.get('lat') or not details.get('lon')):
        try:
            print("Decoding plus code:", details['plus_code'])
            decoded = olc.decode(details['plus_code'])
            details['lat'] = str(decoded.latitudeCenter)
            details['lon'] = str(decoded.longitudeCenter)
        except Exception as e:
            print(f"Could not decode plus code: {e}")
    return details


def extract_latlon(driver):
    # Right-click on the place name to get lat/lon from context menu
    try:
        name_elem = driver.find_element(By.CSS_SELECTOR, '.DUwDvf')
        actions = ActionChains(driver)
        actions.context_click(name_elem).perform()
        time.sleep(1)
        # The coordinates appear in the context menu as the first selectable item
        coord_elem = driver.find_element(By.CSS_SELECTOR, 'ul[role="menu"] li')
        latlon = coord_elem.text
        # Try to parse as lat,lon
        if ',' in latlon:
            lat, lon = latlon.split(',')
            return lat.strip(), lon.strip()
        return latlon, ''
    except Exception as e:
        print(f"Could not extract lat/lon: {e}")
        return '', ''


def search_and_extract(query):
    driver = webdriver.Chrome()
    driver.maximize_window()
    driver.get(GOOGLE_MAPS_URL)
    time.sleep(2)
    # Search
    search_box = driver.find_element(By.ID, 'searchboxinput')
    search_box.clear()
    search_box.send_keys(query)
    search_box.send_keys(Keys.ENTER)
    time.sleep(4)
    # Find all search results
    results = []
    try:
        articles = driver.find_elements(By.CSS_SELECTOR, 'div[role="article"]')
        print(f"Found {len(articles)} results.")
        for idx, article in enumerate(articles):
            try:
                driver.execute_script("arguments[0].scrollIntoView();", article)
                article.click()
                time.sleep(4)
                details = extract_place_details(driver)
                lat, lon = details.get('lat', ''), details.get('lon', '')
                # If lat/lon are still empty, use OpenStreetMap Nominatim API, then Photon as fallback
                if details.get('address'):
                    try:
                        url = "https://nominatim.openstreetmap.org/search"
                        params = {"q": details['address'], "format": "json"}
                        response = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
                        data = response.json()
                        print(details['address'],data)
                        if data:
                            lat = data[0].get('lat', '')
                            lon = data[0].get('lon', '')
                            details['osm_display_name'] = data[0].get('display_name', '')
                            details['osm_place_id'] = data[0].get('place_id', '')
                    except Exception as e:
                        print(f"Could not geocode address with OSM: {e}")
                    # Fallback to Photon if still no lat/lon
                    if (lat=='' or lon==''):
                        lat, lon = geocode_photon(details['address'])
                        if lat and lon:
                            details['photon_source'] = 'photon.komoot.io'
                details['lat'] = lat
                details['lon'] = lon
                details['result_index'] = idx + 1
                results.append(details)
            except Exception as e:
                print(f"Could not click or extract result {idx+1}: {e}")
    except Exception as e:
        print(f"Could not find search results: {e}")
    driver.quit()
    return results




{
  "AssamCities": {
    "Guwahati": [
      "Pan Bazaar",
      "Paltan Bazaar",
      "Fancy Bazaar",
      "Beltola",
      "Ganeshguri",
      "Kahilipara",
      "Six Mile",
      "Chandmari",
      "Hatigaon",
      "Ulubari",
      "Zoo Road",
      "Uzan Bazaar"
    ],
    "Nagaon": [
      "Haibargaon",
      "Dhing",
      "Raha",
      "Samaguri",
      "Kaliabor",
      "Rupohihat",
      "Kampur",
      "Bordowa"
    ],
    "Silchar": [
      "Ambicapatty",
      "Janiganj Bazaar",
      "Gandhibag Park area",
      "Tarapur",
      "Srikona Bara Bazaar",
      "Badarpur",
      "Khaspur",
      "Dolu Lake surroundings"
    ],
    "Dibrugarh": [
      "Duliajan",
      "Naharkatia",
      "Moran",
      "Chabua",
      "Tengakhat",
      "Tingkhong",
      "Lahowal",
      "Rohmoria",
      "Mancotta-Khanikar"
    ],
    "Tinsukia": [
      "Digboi",
      "Margherita",
      "Doom Dooma",
      "Makum",
      "Sadiya",
      "Jagun",
      "Kakopathar",
      "Ledo"
    ],
    "Tezpur": [
      "Agnigarh Hill area",
      "Chitralekha Udyan",
      "Mahabhairab Temple locality",
      "Bhairabi Temple area",
      "Bamuni Hills",
      "Rudrapada Temple area",
      "Kalia Bhomora Setu surroundings",
      "Dhekiajuli"
    ]
  }
}
if __name__ == "__main__":
        import pandas as pd
        import os
        import json
        # AssamCities dict from the bottom of the file
        AssamCities = {
            "Guwahati": [
                "Pan Bazaar", "Paltan Bazaar", "Fancy Bazaar", "Beltola", "Ganeshguri", "Kahilipara", "Six Mile", "Chandmari", "Hatigaon", "Ulubari", "Zoo Road", "Uzan Bazaar"
            ],
            "Nagaon": [
                "Haibargaon", "Dhing", "Raha", "Samaguri", "Kaliabor", "Rupohihat", "Kampur", "Bordowa"
            ],
            "Silchar": [
                "Ambicapatty", "Janiganj Bazaar", "Gandhibag Park area", "Tarapur", "Srikona Bara Bazaar", "Badarpur", "Khaspur", "Dolu Lake surroundings"
            ],
            "Dibrugarh": [
                "Duliajan", "Naharkatia", "Moran", "Chabua", "Tengakhat", "Tingkhong", "Lahowal", "Rohmoria", "Mancotta-Khanikar"
            ],
            "Tinsukia": [
                "Digboi", "Margherita", "Doom Dooma", "Makum", "Sadiya", "Jagun", "Kakopathar", "Ledo"
            ],
            "Tezpur": [
                "Agnigarh Hill area", "Chitralekha Udyan", "Mahabhairab Temple locality", "Bhairabi Temple area", "Bamuni Hills", "Rudrapada Temple area", "Kalia Bhomora Setu surroundings", "Dhekiajuli"
            ]
        }
        location_types=["schools", "colleges", "universities", "medical College","hospitals", "clinics", "pharmacies", "medical stores", "restaurants", "banks", "atm", "supermarkets", "grocery stores", "petrol pumps", "fuel stations", "bus stops", "train stations", "hotels", "lodges", "guest houses", "parks", "playgrounds", "gyms", "fitness centers", "movie theaters", "cinemas", "shopping malls", "markets", "bookstores", "libraries", "post offices", "police stations", "fire stations", "temples", "mosques", "churches", "gurudwaras", "tourist attractions", "museums", "art galleries", "historical sites", "zoo", "aquarium", "gardens", "nature reserves", "wildlife sanctuaries", "bus stations", "railway stations", "airports"," taxi stands","bridges","overbridges","ferries","club","community centers","convention centers","exhibition halls","sports complexes","stadiums","swimming pools","amusement parks","water parks","nightclubs","bars","cafes","bakeries","ice cream parlors","juice bars","salons","spas","yoga centers","meditation centers","court complexes","government offices","municipal offices","public libraries","cultural centers","veterinary clinics","traffic points","weighbridges","industrial areas","business parks","it parks","vehicale showrooms","vehicale service centers","driving schools","repair shops","car rentals"]

        import json
        import re
        def safe_filename(s):
            # Replace spaces and special chars with underscores
            return re.sub(r'[^A-Za-z0-9]+', '_', s.strip())

        for city, areas in AssamCities.items():
            for area in areas:
                for location_type in location_types:
                    query = f"{location_type} in {area}, {city}"
                    print(f"Processing: {query}")
                    all_details = search_and_extract(query)
                    for details in all_details:
                        details['area'] = area
                        details['city'] = city
                        details['location_type'] = location_type
                        print(details)
                    # Save each result set to its own file immediately
                    if all_details:
                        fname = f"results_{safe_filename(location_type)}_{safe_filename(area)}_{safe_filename(city)}.json"
                        with open(fname, 'w', encoding='utf-8') as f:
                            json.dump(all_details, f, ensure_ascii=False, indent=2)
                        print(f"Saved {len(all_details)} results to {fname}")

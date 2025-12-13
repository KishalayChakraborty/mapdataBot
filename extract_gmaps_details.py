import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from openlocationcode import openlocationcode as olc

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
                lat, lon = ['','']#extract_latlon(driver)
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

if __name__ == "__main__":
    import pandas as pd
    query = "schools in maligaon, guwahati"
    all_details = search_and_extract(query)
    for details in all_details:
        print(details)
    # Save to CSV
    import os
    if all_details:
        df = pd.DataFrame(all_details)
        csv_file = "results.csv"
        if os.path.exists(csv_file):
            df.to_csv(csv_file, mode='a', header=False, index=False)
        else:
            df.to_csv(csv_file, index=False)
        print("Appended results to results.csv")

import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import os

# --- CONFIG ---
GOOGLE_MAPS_URL = "https://www.google.com/maps"
OUTPUT_CSV = "../data/results.csv"

def extract_details_from_result(soup):
    # TEMPLATE: Update selectors for real data extraction
    # Example selectors (may need adjustment):
    name = soup.find('h1', {'class': 'DUwDvf'}).text if soup.find('h1', {'class': 'DUwDvf'}) else ''
    address = soup.find('div', {'class': 'Io6YTe'}).text if soup.find('div', {'class': 'Io6YTe'}) else ''
    phone = ''  # Add selector for phone if available
    lat, lng = '', ''  # Add logic to extract lat/lng if possible
    return {
        'name': name,
        'address': address,
        'phone': phone,
        'lat': lat,
        'lng': lng
    }

def scrape_googlemaps_batch(queries):
    driver = webdriver.Chrome()
    results = []
    for query in queries:
        driver.get(GOOGLE_MAPS_URL)
        time.sleep(2)
        # Search for the query
        search_box = driver.find_element(By.ID, 'searchboxinput')
        search_box.clear()
        search_box.send_keys(query)
        search_box.submit()
        time.sleep(3)
def search_and_open_first_result(queries):
    driver = webdriver.Chrome()
    for query in queries:
        driver.get(GOOGLE_MAPS_URL)
        time.sleep(2)
        # Search for the query
        search_box = driver.find_element(By.ID, 'searchboxinput')
        search_box.clear()
        search_box.send_keys(query)
        search_box.submit()
        time.sleep(4)
        # Try to find all results in the left panel
        try:
            results_list = driver.find_elements(By.CSS_SELECTOR, 'div[role="article"], div[aria-label][tabindex="0"]')
            print(f"Query: {query} | Results found: {len(results_list)}")
            if results_list:
                driver.execute_script("arguments[0].scrollIntoView();", results_list[0])
                print(f"First result text: {results_list[0].text}")
                # Try clicking twice with a pause
                driver.execute_script("arguments[0].click();", results_list[0])
                time.sleep(1)
                driver.execute_script("arguments[0].click();", results_list[0])
                print(f"Clicked first result for query: {query}")
                time.sleep(5)
            else:
                print(f"No results found for query: {query}")
        except Exception as e:
            print(f"Error finding/clicking result for query '{query}': {e}")
    driver.quit()

if __name__ == "__main__":
    # Example list of queries
    queries = [
        "Statue of Liberty, New York",
        "Eiffel Tower, Paris",
        "Taj Mahal, India"
    ]
    queries = [
        "schools in maligaon, guwahati",
        "petrol pumps in maligaon, guwahati",
        "atms in maligaon, guwahati"
    ]
    search_and_open_first_result(queries)

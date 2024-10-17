from flask import Flask, jsonify
from bs4 import BeautifulSoup
import threading
import requests

app = Flask(__name__)

# Function to scrape data from the table
def scrape_data(state):
    # Dynamically generate the URL based on the state parameter
    url = f'https://racingaustralia.horse/FreeFields/Calendar_Scratchings.aspx?State={state}'
    
    # Use requests to get the page content
    response = requests.get(url)
    
    if response.status_code != 200:
        return []  # Return empty if there was a problem fetching the page
    
    # Parse the page using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the table with class 'race-fields'
    table = soup.find('table', class_='race-fields')
    
    # List to store scraped data
    scraped_data = []

    if table:
        # Find all rows in the table, skipping the header row
        rows = table.find_all('tr')[1:]

        for row in rows:
            # Extract venue date
            venue_date = row.find_all('td')[0].get_text(strip=True)

            # Extract venue name and link
            venue = row.find_all('td')[1].get_text(strip=True)
            venue_link = row.find_all('td')[1].find('a')['href'] if row.find_all('td')[1].find('a') else None

            # Extract race time
            race_time = row.find_all('td')[2].get_text(strip=True)

            # Store the extracted data
            race_data = {
                'venue_date': venue_date,
                'venue': venue,
                'venue_link': venue_link,
                'race_time': race_time
            }
            scraped_data.append(race_data)

    return scraped_data

# Optimized function to handle scraping in parallel for multiple states (if required)
def scrape_and_save(state):
    scraped_data = scrape_data(state)
    # Removed the database saving part

# Define the Flask route for scraping with a dynamic state parameter
@app.route('/scrape/<state>', methods=['GET'])
def scrape_route(state):
    # Validate the state input
    valid_states = ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']
    
    if state not in valid_states:
        return jsonify({'error': 'Invalid state code'}), 400

    # Start a new thread to scrape the data in the background
    threading.Thread(target=scrape_and_save, args=(state,)).start()
    
    # Perform the scraping and return data immediately
    scraped_data = scrape_data(state)
    
    # Return scraped data immediately without waiting for the background thread
    return jsonify(scraped_data), 200

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)

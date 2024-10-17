from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
from pymongo import MongoClient
import requests

# Set up MongoDB connection
client = MongoClient('mongodb+srv://jackhappy1112:hwoRy3tSgcGxD93N@cluster0.trmdr8u.mongodb.net')  # Use your MongoDB URI
db = client['scrapping']  # Use the database name
collection = db['data']

# Function to save data to the database with partial updates
def save_data_to_db(scraped_data):
    for venue, data in scraped_data.items():
        # Check if the venue exists in the database
        existing_venue = collection.find_one({'venue': venue})
        
        # If the venue does not exist, insert it
        if not existing_venue:
            print(f"Inserting new venue: {venue}")
            collection.insert_one({
                'venue': venue, 
                'race_times': data['race_times']  # Insert all race times as it's a new venue
            })
        else:
            # Ensure race_times exists as an array in the existing venue
            if 'race_times' not in existing_venue or not isinstance(existing_venue['race_times'], list):
                print(f"Initializing race_times array for venue: {venue}")
                collection.update_one(
                    {'venue': venue},
                    {'$set': {'race_times': []}}
                )
                existing_venue['race_times'] = []  # Update local reference as well
            
            # Compare and update race_times for the existing venue
            for new_race in data['race_times']:
                # Find the existing race by race name
                existing_race = next((race for race in existing_venue['race_times'] if race['race'] == new_race['race']), None)

                # If the race does not exist, add the new race
                if not existing_race:
                    print(f"Adding new race {new_race['race']} to venue {venue}")
                    collection.update_one(
                        {'venue': venue},
                        {'$push': {'race_times': new_race}}
                    )
                else:
                    # Compare and update race times
                    if existing_race['time'] != new_race['time']:
                        print(f"Updating race time for {new_race['race']} at venue {venue}")
                        collection.update_one(
                            {'venue': venue, 'race_times.race': new_race['race']},
                            {'$set': {'race_times.$.time': new_race['time']}}
                        )
                    
                    # Update runners only if low, high, or odds are 0 and new values are non-zero
                    for idx, new_runner in enumerate(new_race['runners']):
                        existing_runner = existing_race['runners'][idx]
                        
                        # Only update the low, high, and odds if they are currently 0
                        update_fields = {}
                        if existing_runner['low'] == 0 and new_runner['low'] != 0:
                            update_fields['race_times.$.runners.' + str(idx) + '.low'] = new_runner['low']
                        if existing_runner['high'] == 0 and new_runner['high'] != 0:
                            update_fields['race_times.$.runners.' + str(idx) + '.high'] = new_runner['high']
                        if existing_runner['odds'] == 0 and new_runner['odds'] != 0:
                            update_fields['race_times.$.runners.' + str(idx) + '.odds'] = new_runner['odds']
                        
                        # If any fields need to be updated, run the update query
                        if update_fields:
                            print(f"Updating runner {new_runner['name']} in race {new_race['race']} at venue {venue}")
                            collection.update_one(
                                {'venue': venue, 'race_times.race': new_race['race']},
                                {'$set': update_fields}
                            )

# Set up Selenium with headless Chrome
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run in headless mode

# Path to your ChromeDriver executable
webdriver_service = Service()  # Replace with your actual path
driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)

# Function to scrape the runner details using Selenium
def scrape_race_runners(odds_url):
    base_url = "https://www.thedogs.com.au"
    full_url = base_url + odds_url
    
    driver.get(full_url)
    time.sleep(5)  # Give time for the page to load

    # Get the page source and pass it to BeautifulSoup
    page_content = driver.page_source
    soup = BeautifulSoup(page_content, 'html.parser')

    # Find the table with class "race-runners table--bordered"
    race_table = soup.find('table', class_='race-runners table--bordered')
    runner_data = []

    if race_table:
        # Iterate over each tbody and assign a number to each runner based on the order
        for idx, tbody in enumerate(race_table.find_all('tbody'), start=1):
            runner = {}

            # Assign the runner number based on the order of tbody elements
            runner_number = str(idx)

            # Get runner name
            name_td = tbody.find('td', class_='race-runners__name')
            runner_name = name_td.find('div', class_='race-runners__name__dog').get_text(strip=True) if name_td else 'N/A'

            # Get low odds
            low_td = tbody.find('runner-odd-fluctuation-low')
            low = parse_odds(low_td.find('span', class_='runner-odd__fluctuation').get_text(strip=True)) if low_td else 0

            # Get high odds
            high_td = tbody.find('runner-odd-fluctuation-high')
            high = parse_odds(high_td.find('span', class_='runner-odd__fluctuation').get_text(strip=True)) if high_td else 0

            # Get current odds
            odds_td = tbody.find('runner-odd')
            odds = parse_odds(odds_td.find('span', class_='runner-odd__price').get_text(strip=True)) if odds_td else 0

            # Add data to the runner dictionary
            runner['number'] = runner_number
            runner['name'] = runner_name
            runner['low'] = low
            runner['high'] = high
            runner['odds'] = odds

            # Append to the runner_data list
            runner_data.append(runner)

    return runner_data

# Helper function to parse odds and handle the 'N/A' case
def parse_odds(value):
    try:
        return float(value)
    except ValueError:
        return 0  # Return 0 if the value is not convertible to float

# Function to scrape data from a single table and aggregate race times
def scrape_table(tbody):
    rows = tbody.find_all('tr')
    scraped_data = {}
    
    for row in rows:
        venue_name = row.find('td', class_='meetings-venues__name').get_text(strip=True)
        race_times = []
        for index, race_td in enumerate(row.find_all('td', class_='meetings-venues__race-time'), start=1):
            race_a = race_td.find('a')
            if race_a:
                race_time_text = race_a.get_text(strip=True)
                race_time_link = race_a.get('href', 'N/A')  # Get href or 'N/A' if no link is found
                
                # Remove '?trial=false' from the link if present
                if '?trial=false' in race_time_link:
                    race_time_link = race_time_link.replace('?trial=false', '')
                
                # Append '/odds' to the end of the link if it exists
                if race_time_link != 'N/A':
                    race_time_link = f"{race_time_link}/odds"
                    
                    # Scrape runner details for each race
                    runner_details = scrape_race_runners(race_time_link)
                else:
                    runner_details = []
                
                race_times.append({
                    'race': f"R{index}",
                    'time': race_time_text if race_time_text else '-',
                    'link': race_time_link,
                    'runners': runner_details
                })
        
        if venue_name in scraped_data:
            # Append race times to the existing venue data
            scraped_data[venue_name]['race_times'].extend(race_times)
        else:
            # Create new entry for the venue
            scraped_data[venue_name] = {'venue': venue_name, 'race_times': race_times}
    
    return scraped_data

# Function to perform the scraping
def scrape_data():
    url = 'https://www.thedogs.com.au/racing/scratchings'  # Replace with the actual URL
    
    # Use requests to fetch the page content
    response = requests.get(url)
    response.raise_for_status()  # Ensure the request was successful
    
    # Get the page content and create a BeautifulSoup object
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the first table containing the data (NSW Meetings - Scratchings)
    table = soup.find('table', class_='meeting-grid')
    
    # Get the first tbody from the first table
    tbody = table.find('tbody')

    # Scrape the first tbody (NSW Meetings - Scratchings)
    scraped_data = scrape_table(tbody)
    
    return scraped_data

# Function to run the scraping and saving in a loop
def scrape_periodically(interval_minutes):
    while True:
        print("Starting scraping...")
        scraped_data = scrape_data()
        print("Scraping complete. Saving to database...")
        
        # Save the data to MongoDB
        save_data_to_db(scraped_data)
        
        # Wait for the specified interval
        time.sleep(interval_minutes * 60)

if __name__ == '__main__':
    # Run the scraper every 10 minutes
    interval_minutes = 10
    scrape_periodically(interval_minutes)
    
    # Close Selenium driver when done
    driver.quit()

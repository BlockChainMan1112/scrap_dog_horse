import telebot
import threading
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from pymongo import MongoClient

BOT_TOKEN = "7749287063:AAGimZnkR7-pWtVu4YLyBFtqZcmX4vcAKSo"
bot = telebot.TeleBot(BOT_TOKEN)

client = MongoClient('mongodb+srv://jackhappy1112:hwoRy3tSgcGxD93N@cluster0.trmdr8u.mongodb.net')  # Use your MongoDB URI
db = client['scrapping']  # Use the database name
collection = db['data']

# List of URLs to fetch data from and their corresponding state names
# API_URLS = {
#     "http://127.0.0.1:5000/scrape/NSW": "New South Wales",
#     "http://127.0.0.1:5000/scrape/VIC": "Victoria",
#     "http://127.0.0.1:5000/scrape/QLD": "Queensland",
#     "http://127.0.0.1:5000/scrape/WA": "Western Australia",
#     "http://127.0.0.1:5000/scrape/SA": "South Australia",
#     "http://127.0.0.1:5000/scrape/TAS": "Tasmania",
#     "http://127.0.0.1:5000/scrape/ACT": "Australian Capital Territory",
#     "http://127.0.0.1:5000/scrape/NT": "Northern Territory"
# }

# Flag to control whether to keep fetching data
stop_flag = threading.Event()

# Dictionary to store the last sent data for venues with no scratchings
venues_without_scratchings = {}

# Dictionary to store the last race times for comparison
last_race_times = {}

# Dictionary to store the last sent data per state or venue
last_sent_data = {}

# Function to get additional data from the venue link
# def get_additional_data_from_venue(venue_link):
#     try:
#         if stop_flag.is_set():
#             return ""  # Immediately return if stop_flag is set

#         response = requests.get(venue_link)
#         response.raise_for_status()
#         soup = BeautifulSoup(response.text, 'html.parser')

#         scratchings_heading = soup.find("h3", string="Scratchings")
#         if scratchings_heading:
#             scratching_table = scratchings_heading.find_next("table")
#             rows = scratching_table.find_all("tr")

#             meaningful_rows = []
#             for row in rows:
#                 if stop_flag.is_set():
#                     return ""  # Stop processing immediately if stop_flag is set

#                 columns = row.find_all("td")
#                 if len(columns) > 1:  # Only consider rows with at least 2 columns as valid
#                     meaningful_rows.append(row)

#             if not meaningful_rows:
#                 return ""  # No valid data, return empty string

#             additional_data = "<b><u>Scratchings:</u></b>\n"
#             last_race = None
#             for row in meaningful_rows:
#                 if stop_flag.is_set():
#                     return ""  # Stop processing immediately if stop_flag is set

#                 columns = row.find_all("td")
#                 race_info = columns[0].text.strip()
#                 horse_info = columns[1].text.strip()

#                 if race_info != last_race:
#                     additional_data += f"<b>{race_info}</b> {horse_info}\n"
#                     last_race = race_info
#                 else:
#                     additional_data += f"&#160;&#160;&#160;&#160;{horse_info}\n"

#             return additional_data
#         else:
#             return ""  # No scratchings table found

#     except requests.exceptions.RequestException as e:
#         return f"Failed to fetch additional data: {e}"
#     except Exception as e:
#         return f"Error occurred during scraping: {e}"

# Function to fetch data from APIs
# def fetch_data_from_apis(chat_id):
#     for url, state_name in API_URLS.items():
#         if stop_flag.is_set():
#             return

#         try:
#             response = requests.get(url)
#             if response.status_code == 200:
#                 data = response.json()

#                 df = pd.DataFrame(data)
#                 updated_data = get_updated_entries(state_name, df.to_dict('records'))

#                 if updated_data:
#                     for row in updated_data:
#                         if stop_flag.is_set():
#                             return

#                         race_time = row.get('race_time', 'N/A')
#                         venue = row.get('venue', 'N/A')
#                         venue_date = row.get('venue_date', 'N/A')
#                         venue_link = row.get('venue_link', None)

#                         if venue_link:
#                             additional_data = get_additional_data_from_venue(f"https://racingaustralia.horse{venue_link}")

#                             if additional_data:
#                                 race_schedule = f"State: {state_name}\nRace Time: {race_time} | Venue: {venue} | Date: {venue_date}"
#                                 bot.send_message(chat_id, race_schedule)
#                                 bot.send_message(chat_id, additional_data, parse_mode='HTML')

#         except requests.exceptions.RequestException as e:
#             bot.send_message(chat_id, f"An error occurred with {state_name}: {e}")

# Function to fetch data from MongoDB and only send updates if times have changed
def fetch_data_from_mongodb(chat_id):
    race_data = collection.find()
    for race in race_data:
        if stop_flag.is_set():
            return  # Stop immediately if stop_flag is set

        venue = race.get("venue", "")
        race_times = race.get("race_times", [])

        # Check if the venue already exists in last_race_times
        if venue not in last_race_times:
            last_race_times[venue] = {}

        for race_time in race_times:
            if stop_flag.is_set():
                return  # Stop immediately if stop_flag is set

            time_value = race_time.get("time", "").strip()
            if time_value == "-":
                continue

            race_number = race_time.get("race", "N/A")
            runners = race_time.get("runners", [])

            # Compare current time with the stored time for this race
            if race_number in last_race_times[venue] and last_race_times[venue][race_number] == time_value:
                continue  # No change in time, skip this race

            # Update stored time for this race
            last_race_times[venue][race_number] = time_value

            # Only process runners where the number is valid
            valid_times = []
            try:
                valid_times = [int(time_part) for time_part in time_value.split() if time_part.isdigit()]
            except ValueError:
                pass

            if valid_times:
                # Process and send updated race data only if time is valid
                for runner in runners:
                    runner_name = runner.get("name", "N/A")
                    runner_number = runner.get("number", "N/A")
                    odds = runner.get("odds", "N/A")
                    low = runner.get("low", "N/A")
                    high = runner.get("high", "N/A")

                    # Only send the message if the runner number is in the valid times list
                    if int(runner_number) in valid_times:
                        message = (f"Track: {venue}\n"
                                   f"Race Number: {race_number}\n"
                                   f"Number: {runner_number}\n"
                                   f"Name: {runner_name}\n"
                                   f"Odds: {odds}\n"
                                   f"Low: {low}\n"
                                   f"High: {high}")
                        
                        bot.send_message(chat_id, message)

# Function to handle both API and MongoDB data fetching
def fetch_and_send_data(chat_id, interval):
    while not stop_flag.is_set():
        fetch_data_from_mongodb(chat_id)  # First, process MongoDB data
        # fetch_data_from_apis(chat_id)     # Then, process API data

        # Sleep for the given interval while checking for stop flag
        for _ in range(interval):
            if stop_flag.is_set():
                return  # Stop immediately if stop_flag is set
            time.sleep(1)

# Function to compare data and return only the updated entries
# def get_updated_entries(state_name, new_data):
    # if stop_flag.is_set():
    #     return []

    # if state_name not in last_sent_data:
    #     last_sent_data[state_name] = new_data
    #     return new_data

    # updated_entries = [entry for entry in new_data if entry not in last_sent_data[state_name]]
    # last_sent_data[state_name] = new_data
    # return updated_entries

# Command to start the bot and offer interval options
@bot.message_handler(commands=['start', 'hello'])
def send_welcome(message):
    bot.reply_to(message, "Howdy! Please select how often you'd like me to fetch data:")

    markup = InlineKeyboardMarkup()
    intervals = [10, 20, 30, 40, 50, 60]
    for interval in intervals:
        markup.add(InlineKeyboardButton(f"Every {interval} seconds", callback_data=str(interval)))

    bot.send_message(message.chat.id, "Choose an interval:", reply_markup=markup)

# Handle the user's interval selection
@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    interval = int(call.data)
    bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id,
                          text=f"You selected to fetch data every {interval} seconds.")
    stop_flag.clear()
    threading.Thread(target=fetch_and_send_data, args=(call.message.chat.id, interval)).start()

# Command to stop fetching data
@bot.message_handler(commands=['stop'])
def stop_fetching(message):
    global last_sent_data
    bot.reply_to(message, "Stopping the fetching process and clearing archived data.")
    stop_flag.set()

    # Clear the archived data
    last_sent_data.clear()
    venues_without_scratchings.clear()

# Echo function to reply with the same message
@bot.message_handler(func=lambda msg: True)
def echo_all(message):
    bot.reply_to(message, message.text)

# Start polling for messages
bot.infinity_polling()

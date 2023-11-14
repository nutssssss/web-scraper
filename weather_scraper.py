import threading
import sys
import time
import psutil
from util.UnitConverter import ConvertToSystem
from util.Parser import Parser
from util.Utils import Utils
import requests
import csv
import lxml.html as lh
import config
import contextlib
import os

# Configuration
stations_file = open('stations.txt', 'r')
URLS = stations_file.readlines()
# Date format: YYYY-MM-DD
START_DATE = config.START_DATE
END_DATE = config.END_DATE

# Set to "metric" or "imperial"
UNIT_SYSTEM = config.UNIT_SYSTEM
# Find the first data entry automatically
FIND_FIRST_DATE = config.FIND_FIRST_DATE

# Add a lock to synchronize file creation
file_creation_lock = threading.Lock()

def measure_memory_usage():
    process = psutil.Process()
    return process.memory_info().rss / (1024 ** 2)  # Convert to megabytes

def main():
    global START_DATE
    global END_DATE
    global UNIT_SYSTEM
    global FIND_FIRST_DATE

    stations_file_path = 'stations.txt'
    with open(stations_file_path, 'r') as stations_file:
        urls = stations_file.readlines()

        # Use a lock for file creation
        file_creation_lock = threading.Lock()

        if len(sys.argv) > 1 and sys.argv[1] == "-multi":
            start_memory = measure_memory_usage()
            start_time = time.time()
            multithreaded_scraping(urls, file_creation_lock)
            end_time = time.time()
            end_memory = measure_memory_usage()
            print(f"Multithreaded execution took {end_time - start_time} seconds.")
            print(f"Memory usage: {end_memory - start_memory} MB.")
        else:
            start_memory = measure_memory_usage()
            start_time = time.time()
            for url in urls:
                url = url.strip()
                print(url)
                scrap_station(url, file_creation_lock)
            end_time = time.time()
            end_memory = measure_memory_usage()
            print(f"Single-threaded execution took {end_time - start_time} seconds.")
            print(f"Memory usage: {end_memory - start_memory} MB.")



class FileHandler(contextlib.ContextDecorator):
    def __init__(self, filename, mode='a+'):
        self.filename = filename
        self.mode = mode
        self.file = None

    def __enter__(self):
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        self.file = open(self.filename, self.mode, newline='')
        return self.file

    def __exit__(self, exc_type, exc_value, traceback):
        if self.file:
            self.file.close()

def scrap_station(weather_station_url, file_creation_lock):
    global START_DATE
    global END_DATE
    global UNIT_SYSTEM
    global FIND_FIRST_DATE

    session = requests.Session()
    timeout = 5

    if FIND_FIRST_DATE:
        # Find the first date
        first_date_with_data = Utils.find_first_data_entry(weather_station_url=weather_station_url, start_date=START_DATE)
        # If the first date is found
        if first_date_with_data != -1:
            START_DATE = first_date_with_data

    url_gen = Utils.date_url_generator(weather_station_url, START_DATE, END_DATE)
    station_name = weather_station_url.split('/')[-1]
    file_name = f'data/{station_name}.csv'

    with file_creation_lock, FileHandler(file_name):
        # Create the file if it doesn't exist
        fieldnames = ['Date', 'Time', 'Temperature', 'Dew_Point', 'Humidity', 'Wind', 'Speed', 'Gust', 'Pressure', 'Precip_Rate', 'Precip_Accum', 'UV', 'Solar']

        with open(file_name, 'a+', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write the correct headers to the CSV file
            if UNIT_SYSTEM == "metric":
                writer.writerow({'Date': 'Date', 'Time': 'Time', 'Temperature': 'Temperature_C', 'Dew_Point': 'Dew_Point_C', 'Humidity': 'Humidity_%', 'Wind': 'Wind', 'Speed': 'Speed_kmh', 'Gust': 'Gust_kmh', 'Pressure': 'Pressure_hPa', 'Precip_Rate': 'Precip_Rate_mm', 'Precip_Accum': 'Precip_Accum_mm', 'UV': 'UV', 'Solar': 'Solar_w/m2'})
            elif UNIT_SYSTEM == "imperial":
                writer.writerow({'Date': 'Date', 'Time': 'Time', 'Temperature': 'Temperature_F', 'Dew_Point': 'Dew_Point_F', 'Humidity': 'Humidity_%', 'Wind': 'Wind', 'Speed': 'Speed_mph', 'Gust': 'Gust_mph', 'Pressure': 'Pressure_in', 'Precip_Rate': 'Precip_Rate_in', 'Precip_Accum': 'Precip_Accum_in', 'UV': 'UV', 'Solar': 'Solar_w/m2'})
            else:
                raise Exception("Please set 'unit_system' to either \"metric\" or \"imperial\" ")

            count = 0
            for date_string, url in url_gen:
                try:
                    count += 1
                    if count > 10:
                        break
                    print(f'Scraping data from {url}')
                    history_table = False
                    while not history_table:
                        html_string = session.get(url, timeout=timeout)
                        doc = lh.fromstring(html_string.content)
                        history_table = doc.xpath('//*[@id="main-page-content"]/div/div/div/lib-history/div[2]/lib-history-table/div/div/div/table/tbody')
                        if not history_table:
                            print("Refreshing session")
                            session = requests.Session()

                    # Parse HTML table rows
                    data_rows = Parser.parse_html_table(date_string, history_table)

                    # Convert to the metric system
                    converter = ConvertToSystem(UNIT_SYSTEM)
                    data_to_write = converter.clean_and_convert(data_rows)

                    print(f'Saving {len(data_to_write)} rows')
                    writer.writerows(data_to_write)
                except Exception as e:
                    print(e)


def multithreaded_scraping(urls, file_creation_lock):
    threads = []
    for url in urls:
        url = url.strip()
        thread = threading.Thread(target=scrap_station, args=(url, file_creation_lock))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()



if __name__ == "__main__":
    main()

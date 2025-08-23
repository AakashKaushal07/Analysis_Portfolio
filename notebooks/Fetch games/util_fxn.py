import os
import sys
import json
import traceback
import queue
import threading
from time import sleep
from datetime import datetime, timedelta
import linecache

# Django Imports (ensure your Django environment is set up)
# You might need to add the following at the top of your script if running outside manage.py
# import django
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'your_project.settings')
# django.setup()

from base_app.models import ConfigItems # Assuming you have a 'Game' model to save to
from leagues.models import Competition, Season
from base_app.decorators import cleanup_selenium_instances, timed_retry

# Selenium and Web Scraping Imports
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Data Handling Imports
import gzip
from io import BytesIO
import brotli
import zlib

# --- 1. Thread-Safe Queues for Communication ---
seasons_to_process_queue = queue.Queue()
event_data_queue = queue.Queue()
shot_data_queue = queue.Queue()
# A sentinel value to signal threads to stop
STOP_SIGNAL = "STOP"

# --- 2. Helper & Utility Functions ---

def get_driver(url):
    """Initializes and returns a Selenium WebDriver instance."""
    # To prevent multiple browsers from opening on screen, add headless options
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')
    # options.add_argument('--no-sandbox')
    # options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    driver.implicitly_wait(10)
    return driver

@timed_retry(3)
def remove_cookie_dialog(driver):
    """Removes the cookie dialog from the page."""
    try:
        sleep(5)
        driver.execute_script("var obj = document.getElementById('onetrust-banner-sdk'); if(obj) { obj.remove(); }")
        # Verify removal
        sleep(1)
        driver.find_element(By.ID, "onetrust-banner-sdk")
        raise Exception("Cookie banner not deleted.")
    except (NoSuchElementException, TimeoutException):
        pass # Banner was not found or already gone, which is the desired state.

# --- 3. Producer 1: Event URL Scraper ---

@cleanup_selenium_instances
def scrape_event_data_for_season(season_url):
    """The core logic from your notebook to scrape event data for one season."""
    driver = None
    try:
        driver = get_driver(season_url)
        remove_cookie_dialog(driver)
        wait = WebDriverWait(driver, 60)
        
        # Expand matchweek dropdown
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.Opta-Exp'))).click()
        
        all_mw_elements = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'ul.Opta-Cf a')))
        total_matchweeks = len(all_mw_elements)
        mw_data = []

        for i in range(total_matchweeks):
            # Re-fetch elements to avoid stale references
            all_mw_elements = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'ul.Opta-Cf a')))
            mw_element = all_mw_elements[i]
            mw_name = mw_element.text.strip() or f"Round {i+1}"
            mw_element.click()
            print(f"EVENT PRODUCER: Scraping Matchweek: {mw_name}")
            
            wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'tbody.Opta-result.Opta-fixture')))
            fixture_elements = [elem for elem in driver.find_elements(By.CSS_SELECTOR, 'tbody.Opta-result.Opta-fixture') if elem.text]
            
            for fixture_elem in fixture_elements:
                temp = {}
                # Use try-except blocks for each data point to make it more robust
                try:
                    temp['round'] = mw_name
                    temp['home_team'] = fixture_elem.find_element(By.CSS_SELECTOR, ".Opta-Team.Opta-TeamName.Opta-Home").text
                    temp['away_team'] = fixture_elem.find_element(By.CSS_SELECTOR, ".Opta-Team.Opta-TeamName.Opta-Away").text
                    
                    match_date_str = fixture_elem.find_element(By.CSS_SELECTOR, '.Opta-Date').text
                    match_dt = datetime.strptime(match_date_str.strip(), "%d/%m/%Y %H:%M")
                    
                    # Only process matches that have already happened
                    if match_dt < datetime.now():
                         # Click to go to the match details page
                        fixture_elem.find_element(By.CSS_SELECTOR, '[title="View match"]').click()
                        remove_cookie_dialog(driver)
                        
                        # Find the 'PLAYER STATS' link
                        stats_links = [
                            x.find_element(By.TAG_NAME, 'a').get_attribute('href')
                            for x in driver.find_element(By.CSS_SELECTOR, 'ul.striplist').find_elements(By.TAG_NAME, 'li')
                            if 'PLAYER STATS' in x.text
                        ]
                        
                        if stats_links:
                            temp['event_link'] = stats_links[0]
                        
                        temp["datetime"] = match_dt
                        mw_data.append(temp)
                        driver.back() # Go back to the fixtures list
                    else:
                        print(f"EVENT PRODUCER: Skipping future match: {temp['home_team']} vs {temp['away_team']}")

                except Exception as e:
                    print(f"EVENT PRODUCER: Error processing a fixture in {mw_name}. Error: {e}")
                    # If we are on a match page and an error occurs, we must go back
                    if "fixtures" not in driver.current_url:
                        driver.back()
                    continue
        return mw_data
    finally:
        if driver:
            driver.quit()

def event_url_producer_worker():
    """Worker function that takes seasons, scrapes them, and puts data into the output queue."""
    while True:
        try:
            season_id, season = seasons_to_process_queue.get(timeout=1)
            print(f"EVENT PRODUCER: Starting season {season.name} ({season_id})")
            event_data = scrape_event_data_for_season(season.season_event_url)
            event_data_queue.put((season_id, event_data))
            seasons_to_process_queue.task_done()
        except queue.Empty:
            # If the queue is empty, it means all seasons have been processed.
            print("EVENT PRODUCER: No more seasons to process. Shutting down.")
            break
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(f"EVENT PRODUCER: A critical error occurred for season {season_id}: {e} on line {exc_tb.tb_lineno}")
            seasons_to_process_queue.task_done() # Mark task as done to not block the system

# --- 4. Producer 2: Shot URL Scraper ---

def parse_fotmob_matches(match_list):
    """Parses the JSON data from FotMob API."""
    base_url = "https://www.fotmob.com" # Hardcode or get from ConfigItems
    collection = []
    for match in match_list:
        try:
            status = match.get('status', {})
            score_str = status.get('scoreStr', '-')
            home_score, away_score = score_str.split('-') if '-' in score_str else ('', '')
            
            temp = {
                'home_team': match.get('home', {}).get('name', ""),
                'home_score': home_score.strip(),
                'away_team': match.get('away', {}).get('name', ""),
                'away_score': away_score.strip(),
                'datetime': datetime.strptime(status.get('utcTime', ""), "%Y-%m-%dT%H:%M:%SZ"),
                'shot_url': base_url + match.get('pageUrl', "")
            }
            collection.append(temp)
        except (KeyError, ValueError, IndexError) as e:
            print(f"SHOT PRODUCER: Could not parse a match. Data: {match}. Error: {e}")
            continue
    return collection

@cleanup_selenium_instances
def fetch_shot_data_for_season(season_url):
    """Fetches shot data for a season by intercepting API calls on FotMob."""
    if 'overview' in season_url:
        season_url = season_url.replace("/overview/", "/matches/")
    
    driver = get_driver(season_url)
    api_data = None
    try:
        # Wait up to 20 seconds for the relevant API request to be captured
        req = driver.wait_for_request("https://www.fotmob.com/api/data/leagues", timeout=20)
        if req and req.response:
            encoding = req.response.headers.get('Content-Encoding')
            body = req.response.body
            if encoding == 'gzip':
                body = gzip.decompress(body)
            elif encoding == 'br':
                body = brotli.decompress(body)
            elif encoding == 'deflate':
                body = zlib.decompress(body)
            
            decoded_body = body.decode('utf-8')
            api_data = json.loads(decoded_body)
    except TimeoutException:
        print(f"SHOT PRODUCER: Timed out waiting for API request on {season_url}")
        return None
    finally:
        driver.quit()

    if api_data and 'matches' in api_data and 'allMatches' in api_data['matches']:
        return parse_fotmob_matches(api_data['matches']['allMatches'])
    else:
        print(f"SHOT PRODUCER: Could not find match data in API response for {season_url}")
        return None


def shot_url_producer_worker():
    """Worker function that takes seasons, fetches shot data, and puts it into the output queue."""
    # This producer shares the same input queue as the event producer
    while True:
        try:
            # We need a new queue for shot producers or duplicate seasons in the main queue
            # For simplicity, let's just re-use. In a larger system, separate queues might be better.
            season_id, season = seasons_to_process_queue.get(timeout=1)
            print(f"SHOT PRODUCER: Starting season {season.name} ({season_id})")
            if not season.season_shot_url:
                print(f"SHOT PRODUCER: Skipping season {season_id}, no shot URL provided.")
                seasons_to_process_queue.task_done()
                continue
            
            shot_data = fetch_shot_data_for_season(season.season_shot_url)
            shot_data_queue.put((season_id, shot_data))
            seasons_to_process_queue.task_done() # Mark task as done
        except queue.Empty:
            print("SHOT PRODUCER: No more seasons to process. Shutting down.")
            break
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(f"SHOT PRODUCER: A critical error occurred for season {season_id}: {e} on line {exc_tb.tb_lineno}")
            seasons_to_process_queue.task_done()

# --- 5. The Consumer: Matcher & Saver ---

def data_merger_consumer_worker(total_seasons):
    """
    Waits for data from both producer queues, merges it, and saves it to the database.
    """
    event_cache = {}
    shot_cache = {}
    processed_count = 0

    while processed_count < total_seasons:
        try:
            # Check for event data without blocking forever
            season_id, event_data = event_data_queue.get(block=False)
            print(f"CONSUMER: Received event data for season {season_id}")
            event_cache[season_id] = event_data
        except queue.Empty:
            pass # No event data yet, move on

        try:
            # Check for shot data without blocking forever
            season_id, shot_data = shot_data_queue.get(block=False)
            print(f"CONSUMER: Received shot data for season {season_id}")
            shot_cache[season_id] = shot_data
        except queue.Empty:
            pass # No shot data yet, move on

        # Now, check if any season is complete in our caches
        ready_seasons = set(event_cache.keys()) & set(shot_cache.keys())
        
        for s_id in ready_seasons:
            print(f"CONSUMER: Both data sets ready for season {s_id}. Merging and saving...")
            season_event_data = event_cache.pop(s_id)
            season_shot_data = shot_cache.pop(s_id)

            # --- The Merging Logic ---
            for event_match in season_event_data:
                found_match = False
                for shot_match in season_shot_data:
                    # Match based on normalized team names and date (within a 12-hour window)
                    event_teams = {normalize_team_name(event_match['home_team']), normalize_team_name(event_match['away_team'])}
                    shot_teams = {normalize_team_name(shot_match['home_team']), normalize_team_name(shot_match['away_team'])}
                    
                    time_difference = abs(event_match['datetime'] - shot_match['datetime'])

                    if event_teams == shot_teams and time_difference < timedelta(hours=12):
                        # --- We found a match! Prepare data for saving ---
                        defaults = {
                            'round': event_match.get('round', ''),
                            'datetime': event_match.get('datetime'),
                            'home_score': shot_match.get('home_score', ''),
                            'away_score': shot_match.get('away_score', ''),
                            'event_url': event_match.get('event_link', ''),
                            'shot_url': shot_match.get('shot_url', ''),
                            # Add other fields as per your Game model
                        }
                        
                        # Use update_or_create to avoid duplicates
                        game, created = Game.objects.update_or_create(
                            season_id=s_id,
                            home_team=event_match['home_team'],
                            away_team=event_match['away_team'],
                            defaults=defaults
                        )
                        print(f"CONSUMER: {'Created' if created else 'Updated'} game: {game}")
                        found_match = True
                        break # Move to the next event match
                
                if not found_match:
                    print(f"CONSUMER: WARNING - No matching shot data found for event: {event_match['home_team']} vs {event_match['away_team']} on {event_match['datetime']}")

            processed_count += 1
            print(f"CONSUMER: Finished processing season {s_id}. Total progress: {processed_count}/{total_seasons}")
            
        sleep(5) # Wait a bit before checking the queues again to prevent a busy-wait loop

    print("CONSUMER: All seasons processed. Shutting down.")


# --- 6. The Main Controller ---

def main():
    """
    Sets up and runs the entire producer-consumer workflow.
    """
    # Fetch all seasons you want to process
    seasons_to_run = list(Season.objects.filter(competition_id=5)) # Example filter
    total_seasons_to_process = len(seasons_to_run)

    if total_seasons_to_process == 0:
        print("No seasons found to process. Exiting.")
        return

    print(f"Found {total_seasons_to_process} seasons to process.")
    
    # Populate the initial work queue for both producer types
    # This simple approach requires producers to coordinate. A more advanced
    # pattern might have separate queues per producer type.
    for season in seasons_to_run:
        seasons_to_process_queue.put((season.id, season))
        seasons_to_process_queue.put((season.id, season)) # Put it in twice, once for each producer type

    # --- Start the Worker Threads ---
    threads = []
    
    # You can scale the number of producers here
    num_event_producers = 2 
    num_shot_producers = 2

    for _ in range(num_event_producers):
        thread = threading.Thread(target=event_url_producer_worker)
        thread.start()
        threads.append(thread)

    for _ in range(num_shot_producers):
        thread = threading.Thread(target=shot_url_producer_worker)
        thread.start()
        threads.append(thread)
        
    # Start the single consumer thread
    consumer_thread = threading.Thread(target=data_merger_consumer_worker, args=(total_seasons_to_process,))
    consumer_thread.start()
    threads.append(consumer_thread)

    # --- Wait for all threads to complete ---
    print("Main thread: Waiting for all worker threads to complete...")
    for thread in threads:
        thread.join()

    print("All tasks completed. Workflow finished.")

if __name__ == "__main__":
    main()
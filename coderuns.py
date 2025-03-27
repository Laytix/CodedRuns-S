import json
import random
import requests
from typing import List, Dict, Optional
from datetime import datetime, timezone
from pathlib import Path
import time


session = requests.Session()

failednames = []
# Configuration
INPUT_FILE = "testing.json"
OUTPUT_FILE = "scraped_data.json"
STATE_FILE = "scrape_state.json"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds between retries
REQUEST_DELAY = (1, 5)  # random delay between requests in seconds


def load_state():
    """Load scraping progress state"""
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        with open(INPUT_FILE, "r") as f:
            users = json.load(f)
        return {
            "processed": [],
            "remaining": [user["username"] for user in users],
            "failed": []
        }


def save_state(state):
    """Save scraping progress state"""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def load_scraped_data():
    """Load existing scraped data"""
    try:
        with open(OUTPUT_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_scraped_data(data):
    """Save scraped data incrementally"""
    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=2)

def get_current_age(birthdate):
    # Birthdate in ISO 8601 format

    # Parse the birthdate into a datetime object
    birthdate_dt = datetime.fromisoformat(birthdate.replace("Z", "+00:00"))

    # Get the current datetime in UTC
    now = datetime.now(timezone.utc)

    # Calculate the age
    age = now.year - birthdate_dt.year

    # Adjust if the birthday hasn't occurred yet this year
    if (now.month, now.day) < (birthdate_dt.month, birthdate_dt.day):
        age -= 1

    return age

def get_services(userid):
    try:
        time.sleep(1)
        s_url = f"https://api.codedruns.com/home/services/{userid}"
        response = session.get(s_url)
        response_data = json.loads(response.text)

        return response_data["data"]
    except Exception as e:
        return []


def scrape_user(username):
    """Scrape a single user with retry logic"""
    url = f"https://api.codedruns.com/home/{username}"

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            coderunsdata = json.loads(response.text)
            try:
                coderunsUser = coderunsdata["data"]
                coderunsUser["age"] = get_current_age(coderunsUser["dateOfBirth"])
                coderunsServices = get_services(coderunsUser["id"])
                coderunsUser = {**coderunsUser, "services": coderunsServices}

                return coderunsUser
            except:
                print(f"User Info unavailable")
                return None

        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed for {username}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)

    return None


def main():
    # Initialize data and state
    scraped_data = load_scraped_data()
    state = load_state()

    try:
        while state["remaining"]:
            username = state["remaining"][0]

            # Scrape user data
            user_data = scrape_user(username)

            if user_data:
                scraped_data.append(user_data)
                state["processed"].append(username)
                print(f"Successfully scraped {username}")
                print(f"Users remaining {len(state["remaining"])}")
            else:
                print("reached failed state")
                state["failed"].append(username)
                print(f"Permanently failed to scrape {username}")

            # Update remaining users
            state["remaining"] = state["remaining"][1:]

            # Save progress periodically
            if len(state["processed"]) % 10 == 0:
                save_scraped_data(scraped_data)
                save_state(state)
                print("Progress saved")

            # Random delay between requests
            time.sleep(random.uniform(*REQUEST_DELAY))

        # Final save
        save_scraped_data(scraped_data)
        save_state(state)
        print("Scraping completed successfully!")

    except KeyboardInterrupt:
        print("\nInterrupt received. Saving progress...")
        save_scraped_data(scraped_data)
        save_state(state)
        print("Progress saved. Safe to exit.")
    except Exception as e:
        print(f"Critical error: {str(e)}. Saving progress...")
        save_scraped_data(scraped_data)
        save_state(state)
        print("Progress saved. Please check error.")


if __name__ == "__main__":
    # Create files if they don't exist
    Path(INPUT_FILE).touch(exist_ok=True)
    main()
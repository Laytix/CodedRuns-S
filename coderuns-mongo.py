import random
import requests
from datetime import datetime, timezone
from pymongo import MongoClient, errors
import time
import json
session = requests.Session()

# MongoDB Atlas configuration
MONGO_URI = "mongodb+srv://david:layiwola@david-cluster.p2ehm.mongodb.net/?retryWrites=true&w=majority&appName=david-cluster"
DB_NAME = "coderuns"
TESTING_COLLECTION = "users"
USERS_COLLECTION = "escorts"
STATE_COLLECTION = "scrape_states"

# Initialize MongoDB client
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
testing_collection = db[TESTING_COLLECTION]
users_collection = db[USERS_COLLECTION]
state_collection = db[STATE_COLLECTION]

# Create indexes
users_collection.create_index("slug", unique=True)
state_collection.create_index("scraper_id", unique=True)

# Configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds between retries
REQUEST_DELAY = (2, 6)  # random delay between requests in seconds
SCRAPER_ID = "main_scraper"  # Unique identifier for this scraper instance


def load_state():
    """Load scraping progress state from MongoDB"""
    state = state_collection.find_one({"scraper_id": SCRAPER_ID})

    if not state:
        # Initialize state from testing collection
        testing_users = list(testing_collection.find({}, {"slug": 1}))
        if not testing_users:
            raise ValueError("No users found in testing collection")

        state = {
            "scraper_id": SCRAPER_ID,
            "processed": [],
            "remaining": [user["slug"] for user in testing_users],
            "failed": []
        }
        state_collection.insert_one(state)

    return state


def save_state(state):
    """Save scraping progress state to MongoDB"""
    state_collection.update_one(
        {"scraper_id": SCRAPER_ID},
        {"$set": {
            "processed": state["processed"],
            "remaining": state["remaining"],
            "failed": state["failed"]
        }},
        upsert=True
    )


def get_current_age(birthdate):
    birthdate_dt = datetime.fromisoformat(birthdate.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    age = now.year - birthdate_dt.year
    if (now.month, now.day) < (birthdate_dt.month, birthdate_dt.day):
        age -= 1
    return age


def get_services(userid):
    try:
        time.sleep(0.5)
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
                coderunsUser["services"] = coderunsServices
                coderunsUser["_id"] = coderunsUser["id"]  # Use original ID as MongoDB _id
                coderunsUser["scraped_at"] = datetime.now(timezone.utc)
                return coderunsUser
            except KeyError:
                print(f"User info unavailable for {username}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed for {username}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)

    return None


def main():
    state = load_state()

    try:
        while state["remaining"]:
            username = state["remaining"][0]

            # Check if user exists in MongoDB
            if users_collection.find_one({"slug": username}):
                print(f"{username} already exists in database, skipping")
                state["processed"].append(username)
                state["remaining"] = state["remaining"][1:]
                save_state(state)
                continue

            # Scrape user data
            user_data = scrape_user(username)

            if user_data:
                try:
                    # Insert into MongoDB
                    users_collection.insert_one(user_data)
                    state["processed"].append(username)
                    print(f"Successfully scraped {username}")
                    print(f"Users remaining: {len(state['remaining']) - 1}")
                except errors.DuplicateKeyError:
                    print(f"Duplicate detected for {username}, skipping")
                except Exception as e:
                    print(f"Failed to insert {username}: {str(e)}")
                    state["failed"].append(username)
            else:
                state["failed"].append(username)
                print(f"Permanently failed to scrape {username}")

            # Update remaining users
            state["remaining"] = state["remaining"][1:]

            # Save progress periodically
            if len(state["processed"]) % 10 == 0:
                save_state(state)
                print("Progress saved to MongoDB")

            # Random delay between requests
            time.sleep(random.uniform(*REQUEST_DELAY))

        # Final save
        save_state(state)
        print("Scraping completed successfully!")

    except KeyboardInterrupt:
        print("\nInterrupt received. Saving progress...")
        save_state(state)
        print("Progress saved. Safe to exit.")
    except Exception as e:
        print(f"Critical error: {str(e)}. Saving progress...")
        save_state(state)
        print("Progress saved. Please check error.")


if __name__ == "__main__":
    main()
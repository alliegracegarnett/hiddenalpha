import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Generator
import aiohttp

# Logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# Constants
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAOqZxwEAAAAANF8FtxeB%2FmNN5ZFBgYgDiiFdJYI%3D45oYjiEKKfzehLts6zxwunz8mwnEuVoXo4X0Q6p3XBfag8Usjv"
HEADERS = {"Authorization": f"Bearer {BEARER_TOKEN}", "User-Agent": "FetchTweets/1.0"}
RELEVANT_FILE = Path("data/accounts_relevant.json")
ALL_TWEETS_FILE = Path("data/all_tweets.json")
MAX_TWEETS_PER_USER = 5
DAYS_BACK = 7
FIXED_DELAY = 60
INITIAL_PAUSE = 120
RATE_LIMIT_DELAY = 120


class FetchTweets:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.all_tweets: Dict[str, List[Dict]] = {}

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=HEADERS)
        self.load_all_tweets()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def load_accounts(self) -> Generator[Dict, None, None]:
        """
        Load accounts incrementally from the relevant file.
        """
        if RELEVANT_FILE.exists():
            with open(RELEVANT_FILE, "r", encoding="utf-8") as f:
                accounts = json.load(f)
            logging.info(f"Loaded {len(accounts)} accounts from {RELEVANT_FILE}")
            for account in accounts:
                yield account
        else:
            logging.warning("No accounts to fetch tweets for.")

    def load_all_tweets(self):
        if ALL_TWEETS_FILE.exists():
            with open(ALL_TWEETS_FILE, "r", encoding="utf-8") as f:
                self.all_tweets = json.load(f)
        else:
            self.all_tweets = {}

    def save_all_tweets(self):
        """
        Save tweets to `data/all_tweets.json` without overwriting existing data.
        """
        # Load the existing data, if available
        if ALL_TWEETS_FILE.exists():
            with open(ALL_TWEETS_FILE, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
        else:
            existing_data = {}

        # Merge the existing data with new data
        for username, tweets in self.all_tweets.items():
            if username not in existing_data:
                existing_data[username] = tweets
            else:
                # Avoid duplicates
                existing_ids = {tweet["id"] for tweet in existing_data[username]}
                new_tweets = [tweet for tweet in tweets if tweet["id"] not in existing_ids]
                existing_data[username].extend(new_tweets)

        # Remove users with empty tweet lists
        cleaned_data = {user: tweets for user, tweets in existing_data.items() if tweets}

        # Save the cleaned and merged data
        with open(ALL_TWEETS_FILE, "w", encoding="utf-8") as f:
            json.dump(cleaned_data, f, indent=2, ensure_ascii=False)

    async def handle_rate_limit(self, response: aiohttp.ClientResponse):
        """
        Handle rate limit responses by sleeping for a longer duration after a 429 error.
        """
        if response.status == 429:
            logging.warning(f"Rate limit reached. Pausing for {RATE_LIMIT_DELAY} seconds.")
            await asyncio.sleep(180)

    def get_latest_collected_time(self, username: str) -> Optional[str]:
        """
        Get the latest 'created_at' timestamp of tweets already collected for a given user.
        Returns the timestamp as an ISO8601 string or None if no tweets are collected yet.
        """
        if username not in self.all_tweets or not self.all_tweets[username]:
            return None  # No tweets collected for this user yet

        # Find the latest timestamp among the collected tweets
        latest_time = max(tweet['created_at'] for tweet in self.all_tweets[username])
        return latest_time

    @staticmethod
    def format_time_rfc3339(time_str: str) -> str:
        """
        Ensure the time is properly formatted in RFC 3339.
        """
        dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    async def fetch_user_tweets(self, user_id: str, since: str) -> List[Dict]:
        url = f"https://api.twitter.com/2/users/{user_id}/tweets"
        params = {
            "max_results": MAX_TWEETS_PER_USER,
            "tweet.fields": "created_at,text,id",
            "exclude": "retweets,replies",
            "start_time": since  # Start fetching tweets from this time
        }
        try:
            async with self.session.get(url, params=params) as response:
                await self.handle_rate_limit(response)
                response.raise_for_status()
                data = await response.json()
                return data.get("data", [])
        except Exception as e:
            logging.error(f"Error fetching tweets for user {user_id}: {e}")
            return []

    async def process_account(self, account: Dict):
        """
        Process a single account by fetching its tweets.
        """
        user_id = account.get("id")
        username = account.get("username", f"User_{user_id}")
        if not user_id:
            logging.warning(f"Skipping account with missing ID: {account}")
            return

        # Determine the latest collection time for this user
        latest_collected_time = self.get_latest_collected_time(username)

        # Fetch tweets from the latest collected time, or default to the past 7 days
        since_time = latest_collected_time if latest_collected_time else (
            datetime.utcnow() - timedelta(days=DAYS_BACK)).isoformat() + "Z"

        # Ensure since_time is in RFC 3339 format
        since_time = self.format_time_rfc3339(since_time)

        logging.debug(f"Fetching tweets for @{username} since {since_time}.")

        # Fetch tweets since the latest collected time
        tweets = await self.fetch_user_tweets(user_id, since_time)
        if tweets:
            logging.info(f"Fetched {len(tweets)} new tweets for @{username}.")
            # Avoid duplicates by checking tweet IDs
            existing_ids = {tweet["id"] for tweet in self.all_tweets.get(username, [])}
            new_tweets = [tweet for tweet in tweets if tweet["id"] not in existing_ids]

            # Merge new tweets with already collected ones
            self.all_tweets[username] = self.all_tweets.get(username, []) + new_tweets

            # Save tweets to file
            self.save_all_tweets()
        else:
            logging.info(f"No new tweets found for @{username}.")

    async def crawl_forever(self):
        """
        Continuously fetch tweets from accounts at a steady pace.
        """
        logging.info(f"Initial pause for {INITIAL_PAUSE} seconds to ensure rate limit reset.")
        await asyncio.sleep(300)

        while True:
            for account in self.load_accounts():
                await self.process_account(account)
                logging.info(f"Delaying for {FIXED_DELAY} seconds between requests.")
                await asyncio.sleep(45)

# Clean up all_tweets.json before starting
def clean_all_tweets():
    """
    Remove empty tweet lists from `data/all_tweets.json`.
    """
    if ALL_TWEETS_FILE.exists():
        with open(ALL_TWEETS_FILE, "r", encoding="utf-8") as f:
            all_tweets = json.load(f)

        # Remove users with empty tweet lists
        cleaned_tweets = {user: tweets for user, tweets in all_tweets.items() if tweets}

        # Save cleaned data back to the file
        with open(ALL_TWEETS_FILE, "w", encoding="utf-8") as f:
            json.dump(cleaned_tweets, f, indent=2, ensure_ascii=False)


# Entry point
async def main():
    async with FetchTweets() as fetcher:
        await fetcher.crawl_forever()


if __name__ == "__main__":
    clean_all_tweets()  # Clean the file before starting
    asyncio.run(main())

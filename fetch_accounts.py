#!/usr/bin/env python3
"""
Account Classifier System - Part 1: Core Setup
A system for identifying and classifying social media accounts based on their content
and metrics, specifically focusing on small accounts discussing AI and Web3 topics.
Now includes logic for 30-day irrelevant expiration unless the user has too many followers.
"""

# Standard library imports
import asyncio
import json
import logging
import os
import signal  # <--- Kept even if unused
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# Third-party imports
import aiohttp
from transformers import pipeline

# Configure logging with both file and console handlers
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('account_classifier.log'),
        logging.StreamHandler()
    ]
)

class AccountClassifier:
    """Main classifier class that handles account analysis and classification."""
    
    def __init__(self, bearer_token: str):
        """
        Initialize the account classifier with configurations and state management.
        
        Args:
            bearer_token (str): Twitter API bearer token for authentication
        """
        self.bearer_token = bearer_token
        self.headers = {
            "Authorization": f"Bearer {bearer_token}",
            "User-Agent": "AccountClassifierBot/1.0"
        }
        
        # API Configuration
        self.keywords = ["web3", "AI"]
        self.tweets_per_page = 50
        self.max_pages = 5
        self.tweets_per_user = 5  # Number of recent original tweets to analyze
        
        # Account Relevance Thresholds
        self.max_followers = 2000  # Maximum followers for "small" accounts
        self.min_tweets = 300      # Minimum total tweets to ensure some activity
        self.classification_threshold = 0.8  # LOWER threshold to be more inclusive
        self.relevant_tweet_ratio = 0.4      # 40% of tweets must be relevant
        
        # API Rate Limiting and Retry Configuration
        self.post_cap_monthly = 15000
        self.post_cap_used = 0
        self.requests_made = 0
        self.max_retries = 3
        self.base_delay = 1  # Base delay for exponential backoff
        
        # Initialize ML classifier (using GPU if available, CPU otherwise)
        try:
            self.classifier = pipeline(
                "zero-shot-classification",
                model="facebook/bart-large-mnli",
                device=0
            )
        except Exception as e:
            logging.error(f"Failed to initialize classifier: {e}")
            raise

        # Set up data storage paths
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        self.relevant_file = self.data_dir / "accounts_relevant.json"
        self.irrelevant_file = self.data_dir / "accounts_irrelevant.json"
        
        # Initialize account storage
        self.relevant_accounts: Dict[str, Dict] = {}
        self.irrelevant_accounts: Dict[str, Dict] = {}
        self.load_existing_accounts()
        
        # Initialize aiohttp session (None until __aenter__ is called)
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        """Async context manager entry point that initializes the HTTP session."""
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit that ensures proper cleanup of resources."""
        if self.session and not self.session.closed:
            await self.session.close()

    def load_existing_accounts(self):
        """
        Load previously classified accounts from JSON files.
        
        Then remove any irrelevant accounts older than 30 days EXCEPT those
        that were marked with 'too_many_followers'=True (which is permanent).
        """
        try:
            if self.relevant_file.exists():
                with open(self.relevant_file, 'r', encoding='utf-8') as f:
                    accounts = json.load(f)
                for acc in accounts:
                    self.relevant_accounts[acc["id"]] = acc
                logging.info(f"Loaded {len(self.relevant_accounts)} relevant accounts")
            
            if self.irrelevant_file.exists():
                with open(self.irrelevant_file, 'r', encoding='utf-8') as f:
                    accounts = json.load(f)
                for acc in accounts:
                    self.irrelevant_accounts[acc["id"]] = acc
                logging.info(f"Loaded {len(self.irrelevant_accounts)} irrelevant accounts")

        except json.JSONDecodeError as e:
            logging.error(f"Error parsing account files: {e}")
            raise
        except Exception as e:
            logging.error(f"Error loading existing accounts: {e}")
            raise

        # Age-out irrelevant accounts after 30 days unless flagged "too_many_followers"
        now_utc = datetime.utcnow().replace(microsecond=0, tzinfo=timezone.utc)
        fresh_irrelevant = {}

        for user_id, account_data in self.irrelevant_accounts.items():
            # If permanently irrelevant (large account), skip aging out
            if account_data.get("too_many_followers"):
                fresh_irrelevant[user_id] = account_data
                continue

            classified_str = account_data.get("classified_at", "")
            if not classified_str:
                # No timestamp? keep it
                fresh_irrelevant[user_id] = account_data
                continue
            
            try:
                classified_time = datetime.fromisoformat(classified_str)
            except ValueError:
                # If parsing fails, keep them
                fresh_irrelevant[user_id] = account_data
                continue

            days_elapsed = (now_utc - classified_time).days
            if days_elapsed < 30:
                fresh_irrelevant[user_id] = account_data
            else:
                logging.info(f"Removing user {user_id} from irrelevant (older than 30 days).")

        self.irrelevant_accounts = fresh_irrelevant

    def update_usage_metrics(self, tweets_count: int):
        """
        Track API usage to stay within rate limits and monthly caps.
        Raises an exception if we're approaching or exceeding limits.
        
        Args:
            tweets_count (int): Number of tweets processed in current request
        """
        self.post_cap_used += tweets_count
        self.requests_made += 1
        
        if self.post_cap_used >= self.post_cap_monthly * 0.9:
            logging.warning(f"Approaching monthly post cap: {self.post_cap_used}/{self.post_cap_monthly}")
            
        if self.post_cap_used >= self.post_cap_monthly:
            logging.error("Monthly post cap reached. Stopping operations.")
            raise Exception("Monthly post cap reached")

    async def handle_rate_limit(self, response: aiohttp.ClientResponse, retry_count: int = 0) -> bool:
        """
        Sophisticated rate limit handler with a hybrid approach.
        
        Args:
            response: API response to check for rate limiting
            retry_count: Current retry attempt number
            
        Returns:
            bool: True if we should retry the request, False otherwise
        """
        remaining = response.headers.get('x-rate-limit-remaining', '0')
        limit = response.headers.get('x-rate-limit-limit', '0')
        reset = response.headers.get('x-rate-limit-reset', '0')
        
        if remaining and limit:
            logging.info(f"Rate limits - Remaining: {remaining}/{limit}, Reset: {reset}")

        # If not 429, not rate limited
        if response.status != 429:
            return False

        if retry_count >= self.max_retries:
            logging.error("Maximum retry attempts reached.")
            return False

        current_time = int(time.time())
        if reset:
            reset_time = int(reset)
            wait_time = max(reset_time - current_time, 1)  # Exact wait until reset
        else:
            # Fall back to a default wait time of 60 seconds
            wait_time = 60

        # Cap the wait time at 15 minutes to avoid excessive delays
        wait_time = min(wait_time, 900)
        
        logging.warning(f"Rate limited. Waiting {wait_time} seconds before retrying.")
        await asyncio.sleep(wait_time)
        return True

    async def fetch_recent_tweets(self, keyword: str) -> Tuple[List[Dict], Dict[str, Dict]]:
        """
        Fetch recent tweets matching our search criteria, handling pagination and rate limits.
        Uses fixed delays between pagination requests.

        Args:
            keyword (str): Search term to find relevant tweets

        Returns:
            Tuple[List[Dict], Dict[str, Dict]]: Tweets and user map
        """
        url = "https://api.twitter.com/2/tweets/search/recent"
        params = {
            "query": f"{keyword} -is:retweet -is:reply lang:en",  # Only original English tweets
            "max_results": self.tweets_per_page,  # Keep original page size
            "tweet.fields": "author_id,created_at",
            "expansions": "author_id",
            "user.fields": "public_metrics,description,created_at"
        }

        all_tweets = []
        users_map = {}
        next_token = None
        retry_count = 0

        for page in range(self.max_pages):
            if next_token:
                params["next_token"] = next_token

            while True:
                try:
                    if not self.session:
                        raise RuntimeError("HTTP session not initialized")

                    async with self.session.get(url, params=params) as response:
                        if await self.handle_rate_limit(response, retry_count):
                            retry_count += 1
                            continue

                        response.raise_for_status()
                        data = await response.json()

                        tweets = data.get("data", [])
                        if tweets:
                            self.update_usage_metrics(len(tweets))
                            all_tweets.extend(tweets)

                        # Filter users using `includes.users`
                        includes = data.get("includes", {})
                        for user in includes.get("users", []):
                            metrics = user.get("public_metrics", {})
                            if (metrics.get("tweet_count", 0) >= self.min_tweets and
                                    metrics.get("followers_count", 0) < self.max_followers):
                                users_map[user["id"]] = user
                            else:
                                logging.info(f"Skipping user {user['id']} - does not meet metric thresholds.")

                        next_token = data.get("meta", {}).get("next_token")
                        if not next_token:
                            return all_tweets, users_map

                        # Add a fixed delay between pages
                        await asyncio.sleep(60)  # seconds of fixed delay
                        break

                except aiohttp.ClientError as e:
                    logging.error(f"Error fetching tweets: {e}")
                    if retry_count >= self.max_retries:
                        return all_tweets, users_map
                    retry_count += 1
                    await asyncio.sleep(10)  # Retry delay

        return all_tweets, users_map

    async def fetch_user_tweets(self, user_id: str) -> List[str]:
        """
        Fetch the most recent original tweets from a specific user, within the last 7 days.
        Excludes retweets and replies to focus on original content.

        Args:
            user_id (str): Twitter user ID to fetch tweets for

        Returns:
            List[str]: List of tweet texts
        """
        now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        earliest_dt = now_utc - timedelta(days=7)

        start_time = earliest_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        if start_time >= end_time:
            logging.error(f"Invalid time range for user {user_id}: start_time={start_time}, end_time={end_time}")
            return []

        url = f"https://api.twitter.com/2/users/{user_id}/tweets"
        params = {
            "max_results": self.tweets_per_user * 2,
            "tweet.fields": "text,created_at",
            "exclude": "retweets,replies",
            "start_time": start_time,
            "end_time": end_time
        }

        retry_count = 0
        while retry_count < self.max_retries:
            try:
                if not self.session:
                    raise RuntimeError("HTTP session not initialized")
                    
                async with self.session.get(url, params=params) as response:
                    if await self.handle_rate_limit(response, retry_count):
                        retry_count += 1
                        continue
                    
                    response.raise_for_status()
                    data = await response.json()
                    tweets = data.get("data", [])

                    if tweets:
                        self.update_usage_metrics(len(tweets))
                    
                    return [tweet["text"] for tweet in tweets[:self.tweets_per_user]]
                
            except aiohttp.ClientError as e:
                logging.error(f"Error fetching user tweets for {user_id}: {e}")
                retry_count += 1
                if retry_count < self.max_retries:
                    await asyncio.sleep(self.base_delay * (2 ** retry_count))

        return []  # Removed the redundant params and retry logic

        params = {
            "max_results": self.tweets_per_user * 2,  # Fetch extra to account for filtering
            "tweet.fields": "text,created_at",
            "exclude": "retweets,replies",
            "start_time": start_time,
            "end_time": now_utc
        }

        retry_count = 0
        while retry_count < self.max_retries:
            try:
                if not self.session:
                    raise RuntimeError("HTTP session not initialized")
                    
                async with self.session.get(url, params=params) as response:
                    if await self.handle_rate_limit(response, retry_count):
                        retry_count += 1
                        continue
                    
                    response.raise_for_status()
                    data = await response.json()
                    tweets = data.get("data", [])

                    # Filter tweets for those within the desired range and update metrics
                    if tweets:
                        self.update_usage_metrics(len(tweets))
                    
                    # Return up to 'tweets_per_user' most recent tweets
                    return [tweet["text"] for tweet in tweets[:self.tweets_per_user]]
                
            except aiohttp.ClientError as e:
                logging.error(f"Error fetching user tweets for {user_id}: {e}")
                retry_count += 1
                if retry_count < self.max_retries:
                    await asyncio.sleep(self.base_delay * (2 ** retry_count))

        return []

    def meets_metric_thresholds(self, user_data: Dict) -> bool:
        """
        Evaluate if a user meets our engagement thresholds.
        We're looking for small accounts (follower count < self.max_followers)
        that are consistently active (tweet_count > self.min_tweets).
        """
        metrics = user_data.get("public_metrics", {})
        followers = metrics.get("followers_count", 0)
        tweet_count = metrics.get("tweet_count", 0)
        
        return (followers < self.max_followers) and (tweet_count > self.min_tweets)

    def classify_tweets(self, tweets: List[str]) -> bool:
        """
        Analyze tweet content using zero-shot classification to determine relevance.
        Looks for discussion of AI/Web3 (and related) topics with high confidence.
        
        Args:
            tweets (List[str]): List of tweet texts to analyze
        """
        if not tweets:
            return False
            
        try:
            # We expanded the categories to catch more AI/Web3 synonyms
            categories = [
                "web3",
                "blockchain",
                "defi",
                "stablecoins",
                "cryptocurrency",
                "nft",
                "smart contracts",
                "depin",
                "artificial intelligence",
                "machine learning",
                "neural networks",
                "metaverse",
                "dao",
                "layer 2",
                "tokenomics",
                "distributed ledger",
                "digital identity",
                "gamefi",
                "staking"
            ]
            
            results = self.classifier(
                sequences=tweets,
                candidate_labels=categories,
                multi_label=True,
                hypothesis_template="This tweet discusses {}."
            )
            
            relevant_tweets = 0
            for result in results:
                # If any category confidence > self.classification_threshold => relevant
                if any(score > self.classification_threshold for score in result["scores"]):
                    relevant_tweets += 1
                    
            # Must meet 40% threshold
            return relevant_tweets >= len(tweets) * self.relevant_tweet_ratio
        except Exception as e:
            logging.error(f"Classification error: {e}")
            return False

    def save_account(self, user_data: Dict, relevant: bool):
        """
        Save or update account classification (relevant or irrelevant).
        Always writes to file, removing any old classification if needed.
        
        If user is irrelevant because they exceed max_followers,
        we set user_data["too_many_followers"] = True to keep them from aging out.
        """
        try:
            user_id = user_data["id"]
            user_data["classified_at"] = datetime.now(timezone.utc).isoformat()
            
            if relevant:
                if user_id in self.irrelevant_accounts:
                    del self.irrelevant_accounts[user_id]
                
                # Clear "too_many_followers" if it existed
                user_data.pop("too_many_followers", None)
                
                self.relevant_accounts[user_id] = user_data
                with open(self.relevant_file, 'w', encoding='utf-8') as f:
                    json.dump(list(self.relevant_accounts.values()), f, indent=2, ensure_ascii=False)
                
                logging.info(f"Saved user {user_id} as relevant")
            else:
                # Mark if the reason is "too many followers"
                metrics = user_data.get("public_metrics", {})
                followers = metrics.get("followers_count", 0)
                if followers >= self.max_followers:
                    user_data["too_many_followers"] = True

                if user_id in self.relevant_accounts:
                    del self.relevant_accounts[user_id]
                
                self.irrelevant_accounts[user_id] = user_data
                with open(self.irrelevant_file, 'w', encoding='utf-8') as f:
                    json.dump(list(self.irrelevant_accounts.values()), f, indent=2, ensure_ascii=False)
                
                logging.info(f"Saved user {user_id} as irrelevant")

        except Exception as e:
            logging.error(f"Error saving account {user_data.get('id','unknown')}: {e}")

    async def process_user(self, user_data: Dict):
        """
        Process a single user through our classification pipeline.
        Includes a fixed delay after processing each user.
        """
        user_id = user_data["id"]

        # Check basic metrics
        if not self.meets_metric_thresholds(user_data):
            logging.info(f"User {user_id} fails metric thresholds.")
            self.save_account(user_data, relevant=False)
            await asyncio.sleep(30)  # Fixed delay after processing a user
            return

        # Fetch recent original tweets (within 7 days)
        recent_tweets = await self.fetch_user_tweets(user_id)
        if len(recent_tweets) == 0:
            logging.info(f"Skipping user {user_id} - no original tweets found.")
            await asyncio.sleep(120)  # Fixed delay after processing a user
            return  # Early exit without saving to irrelevant accounts

        # Classify content
        is_relevant = self.classify_tweets(recent_tweets)
        if is_relevant:
            logging.info(f"Classified user {user_id} as relevant.")
            self.save_account(user_data, relevant=True)
        else:
            logging.info(f"Classified user {user_id} as irrelevant.")
            self.save_account(user_data, relevant=False)

        await asyncio.sleep(30)  # 30 seconds fixed delay

    async def run(self):
        """
        Main execution loop that processes all keywords and accounts.
        Includes fixed delays between keyword searches.
        """
        try:
            for keyword in self.keywords:
                logging.info(f"Processing keyword: {keyword}")
                tweets, users_map = await self.fetch_recent_tweets(keyword)
                
                if not tweets:
                    logging.warning(f"No tweets found for keyword: {keyword}")
                    await asyncio.sleep(60)  # Fixed delay between keyword searches
                    continue
                    
                logging.info(f"Found {len(tweets)} tweets and {len(users_map)} unique users.")
                
                # Process each user with fixed delays
                for user_data in users_map.values():
                    await self.process_user(user_data)
                
                # Add a fixed delay before starting the next keyword
                await asyncio.sleep(300)  # seconds fixed delay between keywords
                
        except Exception as e:
            logging.error(f"Error in main execution: {e}")
            raise

def check_existing_process():
    """
    Ensure only one instance of the classifier is running.
    Uses a lock file to track running instances.
    
    Returns:
        bool: True if another instance is running
    """
    lock_file = Path("account_classifier.lock")
    
    if lock_file.exists():
        try:
            with open(lock_file, "r") as f:
                pid = int(f.read().strip())
            
            # Check if the process is still running
            if os.name == 'nt':  # Windows
                import ctypes
                kernel32 = ctypes.windll.kernel32
                process = kernel32.OpenProcess(1, False, pid)
                if process:
                    kernel32.CloseHandle(process)
                    return True
            else:
                os.kill(pid, 0)
                return True
        except (ProcessLookupError, ValueError, OSError):
            lock_file.unlink(missing_ok=True)
    
    # Create new lock file
    with open(lock_file, "w") as f:
        f.write(str(os.getpid()))
    return False

def cleanup_lock():
    """Remove the process lock file during shutdown."""
    lock_file = Path("account_classifier.lock")
    lock_file.unlink(missing_ok=True)

async def main():
    """
    Entry point for the account classification system.
    Handles setup, execution, and cleanup of the classifier.
    """
    bearer_token = os.getenv(
        'TWITTER_BEARER_TOKEN',
        "AAAAAAAAAAAAAAAAAAAAAOqZxwEAAAAANF8FtxeB%2FmNN5ZFBgYgDiiFdJYI%3D45oYjiEKKfzehLts6zxwunz8mwnEuVoXo4X0Q6p3XBfag8Usjv"
    )
    
    # Ensure single instance
    if check_existing_process():
        logging.error("Another instance is already running. Exiting.")
        sys.exit(1)
    
    try:
        start_time = time.strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"Script started at {start_time}")
        
        # Run classifier with proper resource management
        async with AccountClassifier(bearer_token) as classifier:
            await classifier.run()
        
        end_time = time.strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"Script completed successfully at {end_time}")
    except Exception as e:
        logging.error(f"Script failed: {e}")
        raise
    finally:
        cleanup_lock()

if __name__ == "__main__":
    asyncio.run(main())

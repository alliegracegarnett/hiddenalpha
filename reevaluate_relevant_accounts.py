#!/usr/bin/env python3
"""
Reevaluate Relevant Accounts
This script checks `accounts_relevant.json` for:
1. Accounts with zero relevant tweets in the last 30 days.
2. Accounts that have exceeded the follower count threshold (default: 2000).
If either condition is met, accounts are moved to `accounts_irrelevant.json`.
"""

import os
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict

import aiohttp

# File Paths
RELEVANT_FILE = Path("data/accounts_relevant.json")
IRRELEVANT_FILE = Path("data/accounts_irrelevant.json")
ALL_TWEETS_FILE = Path("data/all_tweets.json")

# Constants
DAYS_BACK = 30
MAX_FOLLOWERS = 2000  # Threshold for "too many followers"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

def load_json(file_path):
    """Safely load JSON data from a file."""
    if file_path.exists():
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_json(file_path, data):
    """Save JSON data to a file."""
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

async def fetch_user_metrics(session, bearer_token, user_id):
    """
    Fetch public metrics for a user by ID using Twitter API v2.
    Returns the user object, or {} on failure.
    """
    url = f"https://api.twitter.com/2/users/{user_id}"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "ReclassifyScript/1.0"
    }
    params = {
        "user.fields": "public_metrics"
    }
    try:
        async with session.get(url, headers=headers, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data.get("data", {})
    except Exception as e:
        logging.error(f"Error fetching metrics for user {user_id}: {e}")
        return {}

async def purge_irrelevant_accounts(bearer_token):
    """Reevaluate accounts and purge those with no relevant tweets or too many followers."""
    now = datetime.now(timezone.utc)
    relevant_accounts = load_json(RELEVANT_FILE)
    irrelevant_accounts = load_json(IRRELEVANT_FILE)
    all_tweets = load_json(ALL_TWEETS_FILE)

    remaining_relevant = []
    purged_accounts = []

    async with aiohttp.ClientSession() as session:
        for account in relevant_accounts:
            user_id = account["id"]
            username = account["username"]
            last_checked_at = datetime.fromisoformat(account.get("last_checked_at", "1970-01-01T00:00:00Z"))
            cutoff_time = now - timedelta(days=DAYS_BACK)

            # Check tweet activity in the last 30 days
            tweets = all_tweets.get(username, [])
            recent_relevant_tweets = [
                tweet for tweet in tweets
                if datetime.fromisoformat(tweet["created_at"].replace("Z", "")).replace(tzinfo=timezone.utc) > cutoff_time
            ]

            # Fetch follower count
            new_data = await fetch_user_metrics(session, bearer_token, user_id)
            followers = new_data.get("public_metrics", {}).get("followers_count", 0)

            # Update account metadata
            account["last_relevant_tweet_count"] = len(recent_relevant_tweets)
            account["last_checked_at"] = now.isoformat()

            if len(recent_relevant_tweets) == 0 and (now - last_checked_at).days > DAYS_BACK:
                # Purge due to inactivity
                purged_accounts.append(account)
                logging.info(f"Purging user {user_id} ({username}) - No relevant tweets in 30 days.")
            elif followers >= MAX_FOLLOWERS:
                # Purge due to follower count
                account["too_many_followers"] = True
                purged_accounts.append(account)
                logging.info(f"Purging user {user_id} ({username}) - Exceeded {MAX_FOLLOWERS} followers.")
            else:
                # Keep the account as relevant
                remaining_relevant.append(account)

    # Save updated accounts
    save_json(RELEVANT_FILE, remaining_relevant)
    save_json(IRRELEVANT_FILE, irrelevant_accounts + purged_accounts)

    logging.info(f"Purged {len(purged_accounts)} irrelevant accounts.")
    logging.info(f"Remaining relevant accounts: {len(remaining_relevant)}.")

def main():
    bearer_token = os.getenv(
        'TWITTER_BEARER_TOKEN',
        "AAAAAAAAAAAAAAAAAAAAAOqZxwEAAAAANF8FtxeB%2FmNN5ZFBgYgDiiFdJYI%3D45oYjiEKKfzehLts6zxwunz8mwnEuVoXo4X0Q6p3XBfag8Usjv"
    )
    asyncio.run(purge_irrelevant_accounts(bearer_token))

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Reclassify Relevant Accounts That Grow Too Large

This script checks all users in accounts_relevant.json to see if any
have reached the 'max_followers' threshold (2000 by default).
If so, they are moved to accounts_irrelevant.json with too_many_followers=True.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import aiohttp

# We'll reuse the same threshold from fetch_accounts for consistency
MAX_FOLLOWERS = 2000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

RELEVANT_FILE = Path("data/accounts_relevant.json")
IRRELEVANT_FILE = Path("data/accounts_irrelevant.json")


async def fetch_user_metrics(session: aiohttp.ClientSession, bearer_token: str, user_id: str) -> Dict:
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

async def reclassify_large_accounts(bearer_token: str):
    """
    Loads relevant accounts, checks if any have grown beyond the max_follower threshold,
    and moves them to accounts_irrelevant.json (permanently) if so.
    """
    # Load relevant/irrelevant data
    relevant_accounts = {}
    if RELEVANT_FILE.exists():
        try:
            with open(RELEVANT_FILE, "r", encoding="utf-8") as f:
                arr = json.load(f)
            for acc in arr:
                relevant_accounts[acc["id"]] = acc
        except Exception as e:
            logging.error(f"Could not load relevant file: {e}")

    irrelevant_accounts = {}
    if IRRELEVANT_FILE.exists():
        try:
            with open(IRRELEVANT_FILE, "r", encoding="utf-8") as f:
                arr = json.load(f)
            for acc in arr:
                irrelevant_accounts[acc["id"]] = acc
        except Exception as e:
            logging.error(f"Could not load irrelevant file: {e}")

    if not relevant_accounts:
        logging.info("No relevant accounts to check.")
        return

    async with aiohttp.ClientSession() as session:
        # Check each relevant user to see if they've grown too big
        updated_relevant = {}
        moved_count = 0

        for user_id, user_data in relevant_accounts.items():
            # fetch fresh metrics
            new_data = await fetch_user_metrics(session, bearer_token, user_id)
            if not new_data:
                # If we can't fetch new data, keep them as is for now
                updated_relevant[user_id] = user_data
                continue

            metrics = new_data.get("public_metrics", {})
            followers = metrics.get("followers_count", 0)

            if followers >= MAX_FOLLOWERS:
                # Move to IRRELEVANT permanently
                logging.info(f"User {user_id} has {followers} followers, reclassifying as irrelevant.")
                user_data["too_many_followers"] = True
                user_data["classified_at"] = datetime.now(timezone.utc).isoformat()
                irrelevant_accounts[user_id] = user_data
                moved_count += 1
            else:
                # Remain relevant
                updated_relevant[user_id] = user_data

        # Rewrite relevant file
        with open(RELEVANT_FILE, "w", encoding="utf-8") as f:
            json.dump(list(updated_relevant.values()), f, indent=2, ensure_ascii=False)

        # Rewrite irrelevant file
        with open(IRRELEVANT_FILE, "w", encoding="utf-8") as f:
            json.dump(list(irrelevant_accounts.values()), f, indent=2, ensure_ascii=False)

        logging.info(f"Done reclassifying. Moved {moved_count} accounts to irrelevants.")


def main():
    bearer_token = os.getenv(
        'TWITTER_BEARER_TOKEN',
        "AAAAAAAAAAAAAAAAAAAAAOqZxwEAAAAANF8FtxeB%2FmNN5ZFBgYgDiiFdJYI%3D45oYjiEKKfzehLts6zxwunz8mwnEuVoXo4X0Q6p3XBfag8Usjv"
    )
    asyncio.run(reclassify_large_accounts(bearer_token))

if __name__ == "__main__":
    main()

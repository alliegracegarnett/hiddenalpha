#!/usr/bin/env python3
"""
Re-evaluate Irrelevant Accounts

This script checks each user in accounts_irrelevant.json, ignoring those with
'too_many_followers' = True, and re-runs them through the same classification
pipeline from fetch_accounts.py. If they now qualify as relevant (with new keywords,
etc.), they are moved to accounts_relevant.json.
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

# Import your main classifier from fetch_accounts
from fetch_accounts import AccountClassifier

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

IRRELEVANT_FILE = Path("data/accounts_irrelevant.json")

async def fetch_user_data(
    session: aiohttp.ClientSession,
    bearer_token: str,
    user_id: str
) -> Dict:
    """
    Fetch fresh user data from Twitter (including public_metrics).
    We'll pass this updated info to the classifier logic.
    """
    url = f"https://api.twitter.com/2/users/{user_id}"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "ReevaluateIrrelevantScript/1.0"
    }
    params = {
        "user.fields": "public_metrics,description,created_at"
    }
    try:
        async with session.get(url, headers=headers, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data.get("data", {})
    except Exception as e:
        logging.error(f"Error fetching user data for {user_id}: {e}")
        return {}

async def reevaluate_irrelevant(bearer_token: str):
    """
    Loads each user from accounts_irrelevant.json, skipping those with 'too_many_followers'.
    Fetches fresh user data, then calls the same logic from AccountClassifier
    to see if they might now be relevant (especially with new keywords).
    """
    if not IRRELEVANT_FILE.exists():
        logging.info("No accounts_irrelevant.json found. Nothing to re-check.")
        return

    # Load current irrelevants
    try:
        with open(IRRELEVANT_FILE, 'r', encoding='utf-8') as f:
            accounts_arr = json.load(f)
    except Exception as e:
        logging.error(f"Error loading accounts_irrelevant.json: {e}")
        return

    # For quick access
    irrelevants_dict = {acc["id"]: acc for acc in accounts_arr}

    if not irrelevants_dict:
        logging.info("accounts_irrelevant.json is empty. Nothing to re-check.")
        return

    # Create a new classifier instance (reuse logic from fetch_accounts)
    async with AccountClassifier(bearer_token) as classifier:
        # We can reuse classifier.session for data lookups
        rechecked_count = 0
        for user_id, user_data in list(irrelevants_dict.items()):
            if user_data.get("too_many_followers"):
                # Skip permanently irrelevant
                logging.info(f"Skipping user {user_id} - too_many_followers=True.")
                continue

            # Fetch fresh user data from Twitter
            fresh_data = await fetch_user_data(classifier.session, bearer_token, user_id)
            if not fresh_data:
                # Could not fetch or user not found
                logging.warning(f"User {user_id} not found or no data returned. Skipping.")
                continue

            # Now feed this updated user info into process_user
            # The pipeline internally re-checks thresholds, tweets, classification, etc.
            # But we only do this if we STILL consider them "irrelevant" in memory.
            # The process_user method expects a "user_data" dict with "id" key (the fresh_data).
            # We'll keep some existing fields (like 'id','classified_at'), but fresh_data is the priority.
            fresh_data["id"] = user_id  # ensure "id" is set
            logging.info(f"Re-checking user {user_id} with updated metrics...")

            # This method can automatically save them to relevant if they pass
            await classifier.process_user(fresh_data)
            rechecked_count += 1

        logging.info(f"Re-check completed for {rechecked_count} previously-irrelevant accounts.")

def main():
    bearer_token = os.getenv(
        'TWITTER_BEARER_TOKEN',
        "AAAAAAAAAAAAAAAAAAAAAOqZxwEAAAAANF8FtxeB%2FmNN5ZFBgYgDiiFdJYI%3D45oYjiEKKfzehLts6zxwunz8mwnEuVoXo4X0Q6p3XBfag8Usjv"
    )
    asyncio.run(reevaluate_irrelevant(bearer_token))

if __name__ == "__main__":
    main()

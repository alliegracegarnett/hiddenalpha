#!/usr/bin/env python3
"""
On-demand Account Classification Tool

Prompts for Twitter usernames, fetches their user data and recent tweets,
and uses the same classification logic from fetch_accounts.py.
If a user is relevant, they are saved to accounts_relevant.json;
otherwise, they go to accounts_irrelevant.json.
"""

import asyncio
import os
import sys
import logging
import aiohttp

# Import the same AccountClassifier defined in fetch_accounts.py
from fetch_accounts import AccountClassifier

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

async def fetch_user_by_username(session: aiohttp.ClientSession, username: str) -> dict:
    """
    Use the Twitter API to find a user object by their username.
    Returns the user data (with ID, metrics, etc.) or {} if not found.
    """
    url = f"https://api.twitter.com/2/users/by/username/{username}"
    params = {
        "user.fields": "public_metrics,description,created_at"
    }
    try:
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data.get("data", {})
    except aiohttp.ClientError as e:
        logging.error(f"Error fetching user by username @{username}: {e}")
        return {}

async def classify_usernames(usernames):
    """
    Given a list of Twitter usernames, look each up, then call the same
    classification pipeline from AccountClassifier in fetch_accounts.py.
    """
    bearer_token = os.getenv(
        'TWITTER_BEARER_TOKEN',
        "AAAAAAAAAAAAAAAAAAAAAOqZxwEAAAAANF8FtxeB%2FmNN5ZFBgYgDiiFdJYI%3D45oYjiEKKfzehLts6zxwunz8mwnEuVoXo4X0Q6p3XBfag8Usjv"
    )
    
    # Create an instance of your existing classifier
    async with AccountClassifier(bearer_token) as classifier:
        
        # We'll reuse the classifier's aiohttp session for user lookups, too
        for username in usernames:
            username = username.strip().lstrip('@')  # remove leading '@' if present
            if not username:
                continue
            
            logging.info(f"\nLooking up Twitter user: @{username}")
            user_data = await fetch_user_by_username(classifier.session, username)
            if not user_data or "id" not in user_data:
                logging.warning(f"Could not find user data for '{username}'")
                continue
            
            # Now process that user with classifier's existing pipeline
            await classifier.process_user(user_data)
            logging.info(f"Done processing @{username}.\n")

def main():
    """
    Simple CLI entry point. Prompts for one or more usernames (comma-separated)
    and runs the classification.
    """
    print("Enter one or more Twitter usernames (comma-separated):")
    user_input = input("> ").strip()
    if not user_input:
        print("No usernames entered. Exiting.")
        sys.exit(0)
    
    # Split by comma
    usernames = [u.strip() for u in user_input.split(',') if u.strip()]
    if not usernames:
        print("No valid usernames. Exiting.")
        sys.exit(0)
    
    # Run the async classification
    asyncio.run(classify_usernames(usernames))

if __name__ == "__main__":
    main()

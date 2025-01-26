import requests
import time
import logging

# Configure logging for detailed output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Replace this with your actual X API bearer token
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAOqZxwEAAAAANF8FtxeB%2FmNN5ZFBgYgDiiFdJYI%3D45oYjiEKKfzehLts6zxwunz8mwnEuVoXo4X0Q6p3XBfag8Usjv"
HEADERS = {"Authorization": f"Bearer {BEARER_TOKEN}"}

# Test endpoint (you can change this to test other endpoints)
ENDPOINT = "https://api.twitter.com/2/users/1312971703/tweets"
PARAMS = {"max_results": 5}  # Fetch a small number of tweets per request


def fetch_rate_limit_headers():
    """
    Make a test request to retrieve rate limit headers.
    """
    try:
        response = requests.get(ENDPOINT, headers=HEADERS, params=PARAMS)
        
        # Check response status
        if response.status_code == 200:
            logging.info("Request succeeded.")
        elif response.status_code == 429:
            logging.warning("Rate limit hit.")
        else:
            logging.error(f"Unexpected status code: {response.status_code}")
            logging.error(f"Response: {response.text}")
            return None

        # Extract rate limit headers
        rate_limit_limit = response.headers.get("x-rate-limit-limit", "Unknown")
        rate_limit_remaining = response.headers.get("x-rate-limit-remaining", "Unknown")
        rate_limit_reset = response.headers.get("x-rate-limit-reset", "Unknown")

        return {
            "limit": int(rate_limit_limit) if rate_limit_limit.isdigit() else None,
            "remaining": int(rate_limit_remaining) if rate_limit_remaining.isdigit() else None,
            "reset": int(rate_limit_reset) if rate_limit_reset.isdigit() else None,
        }
    except Exception as e:
        logging.error(f"Error while fetching rate limit headers: {e}")
        return None


def calculate_rate_limits():
    """
    Continuously make requests until rate limits are hit to determine limits.
    """
    logging.info("Starting rate limit determination...")

    requests_made = 0

    while True:
        rate_limits = fetch_rate_limit_headers()
        if not rate_limits:
            logging.error("Failed to fetch rate limit headers. Exiting...")
            break

        limit = rate_limits["limit"]
        remaining = rate_limits["remaining"]
        reset = rate_limits["reset"]

        if limit is None or remaining is None or reset is None:
            logging.warning("Rate limit headers are incomplete. Retrying...")
            time.sleep(5)
            continue

        # Display current rate limit status
        logging.info(f"Rate Limit: {limit}, Remaining: {remaining}, Reset: {reset}")

        # Break if rate limits are hit
        if remaining == 0:
            reset_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(reset))
            logging.warning(f"Rate limit hit. Limit resets at {reset_time}.")
            break

        # Increment request counter
        requests_made += 1

        # Wait briefly between requests to avoid unnecessary load
        time.sleep(2)

    logging.info(f"Total requests made before hitting rate limit: {requests_made}")


if __name__ == "__main__":
    calculate_rate_limits()

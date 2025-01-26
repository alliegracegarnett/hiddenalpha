import requests

# Bearer Token
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAOqZxwEAAAAANF8FtxeB%2FmNN5ZFBgYgDiiFdJYI%3D45oYjiEKKfzehLts6zxwunz8mwnEuVoXo4X0Q6p3XBfag8Usjv"

# Endpoint
URL = "https://api.twitter.com/2/tweets/search/recent"
HEADERS = {"Authorization": f"Bearer {BEARER_TOKEN}"}

def check_rate_limits():
    """Check rate limits for a specific query."""
    params = {
        "query": "AI",  # Replace with any valid query term
        "max_results": 10,  # Limit the number of results for testing
    }
    response = requests.get(URL, headers=HEADERS, params=params)
    if response.status_code == 429:
        print("Rate limit hit:", response.headers)
    elif response.status_code == 200:
        print("Successful request! No rate limit issue.")
        print(response.json())  # Optional: Print fetched tweets
    else:
        print(f"Error: {response.status_code}, {response.text}")

if __name__ == "__main__":
    print("Checking Rate Limits...")
    check_rate_limits()

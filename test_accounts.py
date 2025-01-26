import requests
from datetime import datetime
import json
import time

def handle_rate_limit(response):
    """Handle rate limiting with enhanced exponential backoff"""
    if response.status_code == 429:
        # Get rate limit reset time from headers, default to 15 mins if not found
        reset_time = int(response.headers.get('x-rate-limit-reset', time.time() + 900))
        current_time = time.time()
        
        # Calculate sleep time (add 5 seconds buffer)
        sleep_time = max(reset_time - current_time + 5, 60)
        
        print(f"Rate limit reached. Waiting {int(sleep_time)} seconds...")
        time.sleep(sleep_time)
        return True
    return False

def make_api_request(url, headers, params=None):
    """Make API request with retry logic for rate limits"""
    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 429:
            return response
        if handle_rate_limit(response):
            continue

def test_accounts():
    """
    Test access to Twitter accounts using direct API calls.
    """
    # Set up API access
    bearer_token = "AAAAAAAAAAAAAAAAAAAAAOqZxwEAAAAANF8FtxeB%2FmNN5ZFBgYgDiiFdJYI%3D45oYjiEKKfzehLts6zxwunz8mwnEuVoXo4X0Q6p3XBfag8Usjv"
    headers = {"Authorization": f"Bearer {bearer_token}"}
    
    # Read account usernames
    with open('accounts.txt', 'r') as f:
        accounts = [line.strip() for line in f if line.strip()]
    
    print(f"Testing access to {len(accounts)} accounts...")
    print("-" * 50)
    
    results = []
    
    for username in accounts:
        print(f"\nTesting access to @{username}...")
        
        try:
            # Get user info with retry logic
            user_response = make_api_request(
                f"https://api.twitter.com/2/users/by/username/{username}",
                headers=headers,
                params={"user.fields": "public_metrics"}
            )
            
            if user_response.status_code == 200:
                user_data = user_response.json()
                if 'data' in user_data:
                    user_info = user_data['data']
                    user_id = user_info['id']
                    
                    # Wait between user and tweet requests
                    time.sleep(2)
                    
                    # Get recent tweets with retry logic
                    tweets_response = make_api_request(
                        f"https://api.twitter.com/2/users/{user_id}/tweets",
                        headers=headers,
                        params={
                            "max_results": 100,
                            "tweet.fields": "created_at"
                        }
                    )
                    
                    if tweets_response.status_code == 200:
                        tweets_data = tweets_response.json()
                        tweet_count = len(tweets_data.get('data', []))
                        
                        print(f"✓ Success! Found {tweet_count} recent tweets")
                        if 'public_metrics' in user_info:
                            followers = user_info['public_metrics']['followers_count']
                            print(f"  Follower count: {followers:,}")
                        
                        results.append({
                            'username': username,
                            'success': True,
                            'error': None,
                            'follower_count': followers if 'public_metrics' in user_info else None,
                            'recent_tweets': tweet_count
                        })
                    else:
                        print(f"✗ Failed to get tweets: {tweets_response.status_code}")
                        results.append({
                            'username': username,
                            'success': False,
                            'error': f'Tweet fetch error: {tweets_response.status_code}',
                            'follower_count': None,
                            'recent_tweets': 0
                        })
            else:
                print(f"✗ Failed: {user_response.status_code}")
                results.append({
                    'username': username,
                    'success': False,
                    'error': f'User lookup error: {user_response.status_code}',
                    'follower_count': None,
                    'recent_tweets': 0
                })
            
            # Add a delay between accounts
            if username != accounts[-1]:  # Don't wait after the last account
                wait_time = 10
                print(f"Waiting {wait_time} seconds before next account...")
                time.sleep(wait_time)
                
        except Exception as e:
            print(f"✗ Failed: {str(e)}")
            results.append({
                'username': username,
                'success': False,
                'error': str(e),
                'follower_count': None,
                'recent_tweets': 0
            })
            time.sleep(10)  # Wait after errors
    
    # Print summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    
    successful = sum(1 for r in results if r['success'])
    print(f"\nSuccessfully accessed: {successful}/{len(accounts)} accounts")
    
    if successful < len(accounts):
        print("\nFailed accounts:")
        for result in results:
            if not result['success']:
                print(f"- @{result['username']}: {result['error']}")
    
    # Save detailed results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'account_test_results_{timestamp}.json'
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nDetailed results saved to: {output_file}")

if __name__ == "__main__":
    test_accounts()
import streamlit as st
import re
import json
import html
from datetime import datetime, timezone
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline

# Utility functions
def remove_tco_links(text):
    """Remove unnecessary `t.co` links from the tweet."""
    return re.sub(r'https?://t\.co/\S+', '', text)

def fix_double_ellipses(text):
    """Replace redundant ellipses with a single instance."""
    while "... ..." in text:
        text = text.replace("... ...", "...")
    return text

def time_ago(raw_date):
    """Calculate how long ago a tweet was created."""
    try:
        dt_obj = datetime.strptime(raw_date, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return "Unknown time"

    now = datetime.now(timezone.utc)
    delta = now - dt_obj

    if delta.days > 0:
        return f"{delta.days} day{'s' if delta.days > 1 else ''} ago"
    elif delta.seconds // 3600 > 0:
        return f"{delta.seconds // 3600} hour{'s' if delta.seconds // 3600 > 1 else ''} ago"
    elif delta.seconds // 60 > 0:
        return f"{delta.seconds // 60} minute{'s' if delta.seconds // 60 > 1 else ''} ago"
    else:
        return "Just now"

# Load tweets
try:
    with open("data/all_tweets.json", "r", encoding="utf-8") as f:
        all_tweets = json.load(f)
except FileNotFoundError:
    st.error("ERROR: 'all_tweets.json' not found. Make sure the file exists.")
    st.stop()

# Preload the model
model_name = "facebook/bart-large-mnli"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)

# Initialize classifier
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
LABELS = ["Marketing", "AI", "Crypto"]
THRESHOLD = 0.90

def analyze_tweets(keyword):
    """Analyze tweets based on the selected keyword."""
    relevant_tweets = []

    for username, tweets in all_tweets.items():
        for tw in tweets:
            text = tw.get("text", "").strip()
            if not text:
                continue

            # Perform relevance check
            result = classifier(
                sequences=text,
                candidate_labels=LABELS,
                multi_label=True,
                hypothesis_template="This tweet is about {}."
            )
            label_scores = dict(zip(result["labels"], result["scores"]))

            # Skip irrelevant tweets
            if all(score < THRESHOLD for score in label_scores.values()):
                continue

            raw_date = tw.get("created_at", "")
            t_id = tw.get("id", "")
            if not raw_date or not t_id:
                continue

            # Calculate elapsed time
            elapsed_time = time_ago(raw_date)

            # Clean the tweet text
            text = html.unescape(text)
            text = remove_tco_links(text)
            text = fix_double_ellipses(text)
            if len(text) > 200:
                text = text[:200] + "..."

            # Add categories for the tweet
            categories = [
                label.capitalize() for label, score in label_scores.items()
                if score >= THRESHOLD
            ]

            # Add to relevant tweets if keyword matches or is "All"
            if keyword == "All" or keyword.lower() in [c.lower() for c in categories]:
                relevant_tweets.append({
                    "username": username,
                    "text": text,
                    "elapsed_time": elapsed_time,
                    "link": f"https://x.com/{username}/status/{t_id}",
                    "categories": categories,
                    "timestamp": raw_date  # For sorting
                })

    # Sort tweets by most recent
    relevant_tweets.sort(key=lambda x: x["timestamp"], reverse=True)
    return relevant_tweets

# UI
header = st.container()
content = st.container()

with header:
    st.title("Find Hidden Alpha")
    st.markdown(
        "<div style='font-size:18px; margin-bottom:10px;'>Discover what X isn't showing you.</div>",
        unsafe_allow_html=True
    )

with content:
    # Sidebar for category selection
    st.sidebar.markdown(
        "<div style='font-size:22px; font-weight:bold; margin-bottom:5px;'>Choose a category:</div>",
        unsafe_allow_html=True
    )
    LABELS_UI = ["All"] + LABELS
    keyword = st.sidebar.selectbox("", LABELS_UI, index=0)

    # Analyze tweets for the selected category
    with st.spinner("Processing..."):
        relevant_tweets = analyze_tweets(keyword)

    # Display tweets
    if not relevant_tweets:
        st.warning(f"No relevant tweets found for **{keyword}**.")
    else:
        for tweet in relevant_tweets:
            categories_display = " ".join(
                [f"<span style='font-size:18px; font-weight:bold; color:green; margin-right:10px;'>#{c}</span>" for c in tweet["categories"]]
            )
            st.markdown(f"""
            <div style="font-size:22px; font-weight:bold; margin-bottom:8px;">
                @{tweet['username']} <span style="color:black;">|</span> {tweet['elapsed_time']}
            </div>
            <div style="margin-bottom:8px;">
                {categories_display}
            </div>
            <div style="margin-bottom:8px; font-size:18px;">
                {tweet['text']}
            </div>
            <div style="font-size:16px; color:#007bff; margin-bottom:20px;">
                <a href="{tweet['link']}" target="_blank">View Tweet</a>
            </div>
            """, unsafe_allow_html=True)

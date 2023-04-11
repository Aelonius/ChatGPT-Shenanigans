import tweepy
import time
import mysql.connector

# Set up Twitter API authentication
consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret, access_token, access_token_secret)
api = tweepy.API(auth)

# Set up MySQL database connection
db = mysql.connector.connect(
    host="",
    user="",
    password="",
    database=""
)

# Define keywords to search for
keywords = ["cybersecurity", "infosec", "security analyst", "penetration testing", "network security", "security operations", "threat intelligence", "incident response", "vulnerability management", "cybersecurity architect", "identity and access management", "security awareness", "security software", "security administrator", "cryptographer", "security compliance", "cybersecurity project manager", "cybersecurity sales", "application security", "cloud security", "data protection", "digital forensics", "endpoint security", "firewall", "information assurance", "malware analysis", "mobile security", "physical security", "risk assessment", "secure coding", "security audit", "security engineering", "security governance", "security policy", "security training", "SOC analyst", "web security"]

# Get the newest tweet ID from the database
cursor = db.cursor()
cursor.execute("SELECT MAX(tweet_id) FROM tweets")
result = cursor.fetchone()
if result[0]:
    newest_tweet_id = result[0]
else:
    newest_tweet_id = None

# Define function to save tweet to MySQL database
def save_tweet(tweet_id, tweet_text, tweet_author, tweet_link):
    cursor = db.cursor()
    sql = "INSERT INTO tweets (tweet_id, tweet_text, tweet_author, tweet_link) VALUES (%s, %s, %s, %s)"
    val = (tweet_id, tweet_text, tweet_author, tweet_link)
    cursor.execute(sql, val)
    db.commit()

# Define a time-out to prevent ratelimiting
ratelimit = 120

# Search for tweets that match the keywords
while True:
    try:
        for keyword in keywords:
            if newest_tweet_id:
                tweets = api.search_tweets(q=keyword, lang="en", count=100, since_id=newest_tweet_id)
            else:
                tweets = api.search_tweets(q=keyword, lang="en", count=100)
            for tweet in tweets:
                # Skip over retweets
                if hasattr(tweet, 'retweeted_status'):
                    continue
                tweet_id = tweet.id_str
                tweet_text = tweet.text
                tweet_author = tweet.author.screen_name
                tweet_link = f"https://twitter.com/{tweet_author}/status/{tweet_id}"
                save_tweet(tweet_id, tweet_text, tweet_author, tweet_link)
                if not newest_tweet_id or tweet_id > newest_tweet_id:
                    newest_tweet_id = tweet_id
    except tweepy.TweepyException as e:
        print(e)
    except Exception as e:
        print(e)
    # Add the delay before the next iteration
    time.sleep(ratelimit)

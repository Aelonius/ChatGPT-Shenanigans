import tweepy
import time
import re
import datetime
import mysql.connector
from keywords import keywords_1, keywords_2, keywords_3, keywords_4
from config import mydb, consumer_key, consumer_secret, access_token, access_token_secret, auth

# Script is intended to gather MySQL data, run a RegEx over the results and then publish those tweets into a new database.

# Define a regular expression to match job offers
job_offer_regex = re.compile(r'\b(hiring|job|career|position|employment|vacancy|recruiting|opening)\b', re.IGNORECASE)

# Get a cursor object
cursor = mydb.cursor()

# Define the SQL query to select tweets from the jobcrawler table
sql = "SELECT * FROM jobcrawler"

# Execute the query
cursor.execute(sql)

# Keep track of the most recent tweet ID processed
last_tweet_id = None

# Run the script every 15 minutes
while True:
    # Define the SQL query to select tweets from the jobcrawler table
    if last_tweet_id is None:
        sql = "SELECT * FROM jobcrawler"
    else:
        sql = "SELECT * FROM jobcrawler WHERE tweet_id > %s"
    # Execute the query
    cursor.execute(sql, (last_tweet_id,))

    # Loop through the tweets and check if they are job offers
    while True:
        try:
            tweet = cursor.fetchone()
            if not tweet:
                break
            tweet_text = tweet[0]
            tweet_id = tweet[1]
            user_name = tweet[2]
            user_screen_name = tweet[3]
            user_location = tweet[4]
            tweet_created_at = tweet[5]
            tweet_language = tweet[6]
            tweet_urls = tweet[7]

            # Check if the tweet is a job offer and not a duplicate
            if job_offer_regex.search(tweet_text) and (last_tweet_id is None or tweet_id > last_tweet_id):
                # If the tweet is a job offer and not a duplicate, insert it into the opportunities table
                insert_sql = "INSERT INTO opportunities (tweet_text, tweet_id, user_name, user_screen_name, user_location, tweet_created_at, tweet_language, tweet_urls) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                insert_values = (tweet_text, tweet_id, user_name, user_screen_name, user_location, tweet_created_at, tweet_language, tweet_urls)
                cursor.execute(insert_sql, insert_values)
                mydb.commit()

                # Update the last_tweet_id to avoid processing duplicates in the next iteration
                last_tweet_id = tweet_id

        except mysql.connector.errors.InternalError:
            # Skip unread result and move to the next set
            cursor.nextset()

    # Wait for 15 minutes before checking for new tweets again
    time.sleep(900)  # 900 seconds = 15 minutes

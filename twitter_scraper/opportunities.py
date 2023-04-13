import time
import re
import datetime
import mysql.connector
from keywords import keywords_1, keywords_2, keywords_3, keywords_4
from config import mydb, consumer_key, consumer_secret, access_token, access_token_secret, auth

# Define a regular expression to match job offers
job_offer_regex = re.compile(r'\b(hiring|job|career|position|employment|vacancy|recruiting|opening)\b', re.IGNORECASE)

# Get a cursor object
cursor = mydb.cursor(buffered=True)

# Define the SQL query to select tweets from the jobcrawler table
sql = "SELECT * FROM jobcrawler"

# define our 2nd insert clause
insert_sql = "INSERT INTO opportunities (tweet_text, tweet_id, user_name, user_screen_name, user_location, tweet_created_at, tweet_language, tweet_urls) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
# Execute the query
cursor.execute(sql)

# Keep track of the most recent tweet ID processed
last_tweet_id = None

# Run the script every 15 minutes, Loop through the tweets and check if they are job offers
while True:
    
    # grab every record from the database
    records = cursor.fetchall()

    # testing to see output from fetchall() func
    for row in records:
        tweet_id = row[0]
        tweet_text = row[1]
        user_name = row[2]
        user_screen_name = row[3]
        user_location = row[5]
        tweet_created_at = row[6]
        tweet_language = row[7]
        tweet_urls = row[4]

        try:
            if job_offer_regex.search(tweet_text):
                insert_values = (tweet_text, tweet_id, user_name, user_screen_name, user_location, tweet_created_at, tweet_language, tweet_urls)
                cursor.execute(insert_sql, insert_values)
                mydb.commit()
        except:
            # the keywords weren't found in the regex search
            continue

    # Wait for 15 minutes before checking for new tweets again
    print("=========================")        
    print("Rate limit exceeded, waiting for 15 minutes.")
    print("=========================")
    time.sleep(900)  # 900 seconds = 15 minutes

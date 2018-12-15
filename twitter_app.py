import socket
import sys
import requests
import requests_oauthlib
import json
import flask

APP_KEY = 'pVqijAxfbDRTWBadqEHs43Ozm'
APP_SECRET = '5n0b7DDeCYCqeiHxk2vDNR6gurMcj3mEBAJQ6xIlStShY3R0DR'
OAUTH_TOKEN = '985071710100598784-NTA548fEqcop86mOBV2bbHUOaDfz3X4'
OAUTH_TOKEN_SECRET = 'uByR9KjsKdqLL5c1rmcUCmC3G9MNesoWwEuZ1Zf5sskLv'
my_auth = requests_oauthlib.OAuth1(APP_KEY, APP_SECRET,OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

def get_tweets():
	url = 'https://stream.twitter.com/1.1/statuses/filter.json'
	query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
	response = requests.get(query_url, auth=my_auth, stream=True)
	print(query_url, response)
	return response

def send_tweets_to_spark(http_resp, tcp_connection):
  for line in http_resp.iter_lines():
    try:
      full_tweet = json.loads(line)
      tweet_text = json.dumps(full_tweet) + '\n'
      # print(full_tweet)
      # if(line['text']):
      #   tcp_connection.send(full_tweet.encode('utf-8'))
      # print(full_tweet)
      # print(type(tweet_text))
      print("Tweet Text: " + tweet_text)
      print ("------------------------------------------")
      tcp_connection.send(tweet_text.encode())
    except:
      e = sys.exc_info()[0]
      print("Error: %s" % e)

TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(5)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)
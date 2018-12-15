# Setup Twitter API using Twython

from twython import Twython, TwythonStreamer

APP_KEY = 'pVqijAxfbDRTWBadqEHs43Ozm'
APP_SECRET = '5n0b7DDeCYCqeiHxk2vDNR6gurMcj3mEBAJQ6xIlStShY3R0DR'
OAUTH_TOKEN = '985071710100598784-NTA548fEqcop86mOBV2bbHUOaDfz3X4'
OAUTH_TOKEN_SECRET = 'uByR9KjsKdqLL5c1rmcUCmC3G9MNesoWwEuZ1Zf5sskLv'

# Hash function: ( id mod 10 ) mod 4

def hash_function(id):
    return ( id % 10) % 4

# Deciding Trending

import operator

def frequent_word(str):
    dict = {}
    str = str.split(' ','.')
    for word in str:
        if word not in ['a', 'the', 'this', 'that', 'but', 'also', 'of', 'for']:
            if word in dict:
                cnt = dict[word] + 1
                dict[word] = cnt
            else:
                dict[word] = 1
            
    sorted_dict = sorted(dict.items(), key=operator.itemgetter(1), reverse=True)
    return sorted_dict

class Streamer(TwythonStreamer):
    
    def on_success(self, data):
        
        # Sample size: 1/4
        # Hash each tuple’s key into 4 buckets (0, 1, 2, 3)
        # Pick the first bucket only (hash = 0)
        
        sample = []
        if 'text' in data:
            temp = hash_function(data['id'])
            if (temp == 0):
                sample.append(data)
                print(frequent_word(data['text']))
            # else:
            #     print("not")
            #     print(temp)


    def on_error(self, status_code, data):
        print("Error Code: ", status_code)
        self.disconnect()

sampling_stream = Streamer(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
sampling_stream.statuses.filter(locations = "-74,40,-73,41")
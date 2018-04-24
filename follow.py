import tweepy
import json
import time
from tweepy import OAuthHandler


Accesstoken = '978447119169105920-iS2pxiVlnlzAqmyCzXxIyixgdThO90S'
AccesstokenSec = 'LqqXDAf0ZDamKfC2EwoHeUBSVCJmUHcEH7Rb6NSLrodmu'
ConsumerKey = 's0KlKDBAskCAv7L0gbeaIMyFj'
ConsumerSec = 'HvATuMNObaazzNUDfUbKVU0flsASXCkDVEQyFDPadcoGtauuk1'

auth = OAuthHandler(ConsumerKey,ConsumerSec)
auth.set_access_token(Accesstoken,AccesstokenSec)

api = tweepy.API(auth)

lines = []
IDS = []


def streamlistener():
    try:
        f = open("pythonID.json","r+")
        jsontext = f.readlines()
        for line in jsontext:
            text = json.loads(line)
            l = str(text['user']['screen_name'])
            lines.append(l)

        return lines
        f.close()
    except BaseException as e:
        print("error")


def follower():
    name = streamlistener()[0]
    for page in tweepy.Cursor(api.followers_ids,screen_name = name).pages():
        IDS.extend(page)
        time.sleep(10)
    return IDS

def main():
    L = []
    for item in follower():
        L.append(item)
    return L

if __name__ == "__main__":
    follower()
    print(IDS)

new_tweets = api.user_timeline(user_id = '195611518',count=10, tweet_mode="extended")
print(new_tweets)

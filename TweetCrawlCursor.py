import time

import tweepy
from tweepy import OAuthHandler


def logp(log_info):
    print('[' + time.strftime("%Y-%m-%d %H:%M:%S") + '] ' + log_info)


accounts = []  # for fetching status
# chuangy
accounts.append({
    'consumer_key': '4eEm0v4afh2MH9juRASmR3xd0',
    'consumer_secret': 'ek5x4au3LBo25iBcwKf9lzJMViz1KCoYApbPIYstHZgWJHHFHY',
    'access_token': '853442542074908672-A3pnyKH3xVGC65s5lvtZnIPMymGL4kb',
    'access_secret': 'UgORHfIhuvCYAH11nwejwupfN92Lu9GKpyBbSc6HAOttT'})

accounts.append({
    'consumer_key': 'Ahm52ivjZ9aLxdF8mGGhhKhKU',
    'consumer_secret': 'eyGbe8yBHendsSOxyoZ99Z0ohMl1NDuJtUG3rL39Ic3V50hkFF',
    'access_token': '853442542074908672-7t6ztvCOsHqClazlbxFKe2p1YTFIfAB',
    'access_secret': '50FpJK7S5Cdabmm6kQr0ikWWe3FY83y3sRZrB8tX265XO'})

# arthur
accounts.append({
    'consumer_key': '7NW9RyZLWnXE7jbbgC7Q5ErJw',
    'consumer_secret': 'Vmkp7ZrAVTVob0Cxr2lGMFokjNKvKLTWQtrVbWmf8FaP9E48Jx',
    'access_token': '4444601909-kdjeiUNVq4eIFpkXUGuRPJUAoQImceVIcPzBAgK',
    'access_secret': 'A7lOd2DZ38SnpUcXNQvd8QT7m16B0Ckm8xuyw55qFrIO9'})
logp('Accounts added')

# add accounts to apis
apis = []
for account in accounts:
    auth = OAuthHandler(account['consumer_key'], account['consumer_secret'])
    auth.set_access_token(account['access_token'], account['access_secret'])
    apis.append(tweepy.API(auth))

tweets = apis[0].home_timeline()
for tweet in tweets:
    print(tweet.text)

import time

import tweepy
from tweepy import OAuthHandler


def logp(log_info):
    print('[' + time.strftime("%Y-%m-%d %H:%M:%S") + '] ' + log_info)


# accounts for tweet api
accounts = []

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

accounts.append({
    'consumer_key': 'NhmAGuRvOhen2EXoReEvFPhn9',
    'consumer_secret': 'q2ZrUQrdInVAHKr2ClVBqv3RfCIB2jO4A2HLm5QifwYmIcoXEu',
    'access_token': '853442542074908672-nAf6QrTJjHasdmXqtghe7FoIz54dRCk',
    'access_secret': 'DGckFXikMtxjmGM16pGCp2pgXGvz7O6hDw0Fe7fRzglXA'})

accounts.append({
    'consumer_key': '7NW9RyZLWnXE7jbbgC7Q5ErJw',
    'consumer_secret': 'Vmkp7ZrAVTVob0Cxr2lGMFokjNKvKLTWQtrVbWmf8FaP9E48Jx',
    'access_token': '4444601909-kdjeiUNVq4eIFpkXUGuRPJUAoQImceVIcPzBAgK',
    'access_secret': 'A7lOd2DZ38SnpUcXNQvd8QT7m16B0Ckm8xuyw55qFrIO9'})
logp('Accounts added')

# add accounts to apis
apis = []
account = accounts[0]
auth = OAuthHandler(account['consumer_key'], account['consumer_secret'])
auth.set_access_token(account['access_token'], account['access_secret'])
apis.append(tweepy.API(auth))


class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        print(status)

    def on_error(self, status):
        print(status)
        return False


myStreamListener = MyStreamListener()
myStream = tweepy.Stream(apis[0].auth, myStreamListener)
myStream.filter(locations=[144.9027, -37.8507, 144.9914, -37.7754], languages=["en"])

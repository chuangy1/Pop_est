import json
import logging
import sys

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

Accesstoken = '978447119169105920-iS2pxiVlnlzAqmyCzXxIyixgdThO90S'
AccesstokenSec = 'LqqXDAf0ZDamKfC2EwoHeUBSVCJmUHcEH7Rb6NSLrodmu'
ConsumerKey = 's0KlKDBAskCAv7L0gbeaIMyFj'
ConsumerSec = 'HvATuMNObaazzNUDfUbKVU0flsASXCkDVEQyFDPadcoGtauuk1'

logger = logging.getLogger(__name__)

auth = OAuthHandler(ConsumerKey, ConsumerSec)
auth.set_access_token(Accesstoken, AccesstokenSec)

api = tweepy.API(auth)


def Readlocation(location_name):
    f = open("Locations.txt", "r")
    lines = f.readlines()
    for line in lines:
        l = line.split(":")
        if l[0] == location_name:
            Lname = l[1].strip().strip('[]')
            coordinate = Lname.split(",")
            return coordinate
    f.close()


class MyListener(StreamListener):
    def on_data(self, data):
        try:
            with open('python2.json', 'a') as f:
                f.write(data)
                print(data)
                text = json.loads(data)
                try:
                    fs = open('pythonID.json', 'a')
                    dictObj = {'user': text['user']}
                    jsObj = json.dumps(dictObj)
                    fs.write(jsObj + '\n')
                    print(jsObj)
                except BaseException as e:
                    print("error on write")
                return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def main(argv):
    L = Readlocation(argv)
    l0 = L[0]
    l1 = L[1]
    l2 = L[2]
    l3 = L[3]

    twitter_stream = Stream(auth, MyListener())
    twitter_stream.filter(locations=[float(l0), float(l1), float(l2), float(l3)])


if __name__ == "__main__":
    main(str(sys.argv[1]))

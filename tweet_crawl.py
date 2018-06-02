import queue
import traceback
from time import sleep, strftime

import couchdb
import tweepy
from tweepy import OAuthHandler


def tlog(log_info):
    print('[' + str(strftime("%Y-%m-%d %H:%M:%S")) + '] ' + str(log_info))


class OrderedSet:
    def __init__(self):
        self.set = set([])
        self.queue = queue.Queue()

    def put(self, i):
        if i in self.set:
            return False
        else:
            self.set.add(i)
            self.queue.put(i)
            return True

    def get(self):
        v = self.queue.get()
        self.set.remove(v)
        return v

    def size(self):
        return self.queue.qsize()

    def empty(self):
        return self.queue.empty()


tlog('start')
# params
couchIP = 'sourcead:iamfine@115.146.95.47'  # IP of master couchdb
# couchIP = 'localhost'
tlog(couchIP)
db_config_name = 't_config'
db_twitter = 'twitter2'
doc_config_name = 'config_doc'
is_stream = True
thread_rank = 0
thread_count = 4
acc_pos = 0
max_status_count = 100
max_friend_count = 2000
un_user_capacity = 30000
uc = 'user_count'
ucg = 'user_count_geo'
tc = 'tweets_count'
tcg = 'tweets_count_geo'
cnt = {uc: 0, ucg: 0, tc: 0, tcg: 0}

# locations = [
#     [144.9027, -37.8507, 144.9914, -37.7754],  # Melbourne City
#     [151.1715, -33.9243, 151.2318, -33.8509],  # Sydney Inner City
#     [152.9962, -27.4938, 153.0548, -27.4493],  # Brisbane Inner
#     [115.7910, -31.9737, 115.8934, -31.9075]  # Perth City
# ]
locations = [
    [144.8704, -38.1094, 145.3103, -37.6978],  # Melbourne
    [150.9321, -34.0078, 151.3097, -33.7969],  # Sydney
    [152.9583, -27.4938, 153.0897, -27.4040],  # Brisbane
    [115.7910, -31.9737, 115.8934, -31.9075]  # Perth
]

initial_screen_name = ["MorryMorgan", "ClintLThomas", "Nat_Edwards", "jackomolloyo", "brucelwoolley", "martin_o",
                       "AustEcologist", "KateLimon9"]

place_list = ['melbourne', 'mel', 'syd', 'sydney', 'aus', 'victoria', 'vic', 'queensland', 'qld', 'new south wales',
              'nsw', 'bne', 'tasmania', 'tas', 'act', 'canberra', 'brisbane', 'perth', 'darwin', 'adelaide', 'hobart']

# accounts for tweet api
accounts = [
    # yingchuang
    {'consumer_key': '4eEm0v4afh2MH9juRASmR3xd0',
     'consumer_secret': 'ek5x4au3LBo25iBcwKf9lzJMViz1KCoYApbPIYstHZgWJHHFHY',
     'access_token': '853442542074908672-A3pnyKH3xVGC65s5lvtZnIPMymGL4kb',
     'access_secret': 'UgORHfIhuvCYAH11nwejwupfN92Lu9GKpyBbSc6HAOttT'},
    {'consumer_key': 'Ahm52ivjZ9aLxdF8mGGhhKhKU',
     'consumer_secret': 'eyGbe8yBHendsSOxyoZ99Z0ohMl1NDuJtUG3rL39Ic3V50hkFF',
     'access_token': '853442542074908672-7t6ztvCOsHqClazlbxFKe2p1YTFIfAB',
     'access_secret': '50FpJK7S5Cdabmm6kQr0ikWWe3FY83y3sRZrB8tX265XO'},
    {'consumer_key': '0QblwtahftHzpUY6JNBKJlYxH',
     'consumer_secret': 'mC7ZnBBKSNipAlmwQ2johuhugx0m3wZCnO2yChjZxxkLn7oDUQ',
     'access_token': '853442542074908672-glcGvuSrkTZmkoppCnbjd9ULLFjourF',
     'access_secret': 'JheGGn1NTR2JwDNVc2lKW64cNfsGOOvAqJ63aU458SVBk'},
    {'consumer_key': 'xe9mVovHE49pll0BedCxckxoj',
     'consumer_secret': 'Buy7Y5eBjbypLveoqEXEj9exCpDspWUCI9hag9Z8Iigd7YAsOJ',
     'access_token': '853442542074908672-BOEyHjWAQFu2IuvJKOGy22qdK32o4Q1',
     'access_secret': 'rufPkPAeJXf1p1xuCSowZGspLFU0AMoQ45q4CyjXWz6PF'},
    {'consumer_key': 'NhmAGuRvOhen2EXoReEvFPhn9',
     'consumer_secret': 'q2ZrUQrdInVAHKr2ClVBqv3RfCIB2jO4A2HLm5QifwYmIcoXEu',
     'access_token': '853442542074908672-nAf6QrTJjHasdmXqtghe7FoIz54dRCk',
     'access_secret': 'DGckFXikMtxjmGM16pGCp2pgXGvz7O6hDw0Fe7fRzglXA'},
    # arthur
    {'consumer_key': 'e4Fv8KAwQ5L5A5NZxqMANJuuz',
     'consumer_secret': 'b3kQg69dKxVcpYMaeaSvkQSaWxLZoFsIp4cQnzU033daWCAUgt',
     'access_token': '854235761587048449-467HV6EMSqsuHbSmrvRICZHoMPuTJUT',
     'access_secret': 'UzX374xOJJBD10Rzm5nBqXw949f4eGUDEt6V7fKRbGdpZ'},
    # kwei
    {'consumer_key': 'B8PeDuAVGycIPn73ceE0vLo1e',
     'consumer_secret': 'daF1UjcwavAnoFiHnNxAmyuJevG2JU9GlEYoTkRLsWiKqWb6fY',
     'access_token': '2575592904-JWrYORjYN0JPX85Q2Cbo5lYTjOpNfiNiT3fQ6Dq',
     'access_secret': '6eaZYdPSXIDA9Sr9TEvTFp0OR4aCFlDjupKPxb99TSAeY'},
    {'consumer_key': '48YW6WZRjyPtMGwR6VX7lBieE',
     'consumer_secret': 'wVes0R25aoNEiLcdfwTlPngZCFaF4AE7eeQpWflZtbJpo3k3eA',
     'access_token': '854237152212688896-CsaNyvQ0eIkfrDuz5TOZVCzdFpv6Peo',
     'access_secret': '6BhuokvlK1LvRH7BLCS7ZkPG2lql5TPV8SXojfeVkRc0i'},
    {'consumer_key': 'BISq0gnPO5sKPmSGIDDX8CoNi',
     'consumer_secret': 'wqwZPU7Eq1YBxSTGagFoy5OHcuYyoJ7cs6Wrg7yz0C8gvki6kU',
     'access_token': '854237152212688896-sjbpQYfsArfzvCKTmSqxjt5d5Lj4Ui4',
     'access_secret': '7AP93iXzTNPlJdtccQLkCb047SPXdJhJ2mIr3gwR0KkUK'},
    {'consumer_key': '0hDkVWYVtI3FbPBO0eabByMJr',
     'consumer_secret': 'IM4IBDHosLw1GrG5rX0pVQy9vv3wxwZdGS630lD7YPpVCZ21n2',
     'access_token': '854237152212688896-lGvQMrSPKA3I8daYWR4q20TCNi5oEah',
     'access_secret': 'UwJ2GjZjAlJmcuHhSk32WPo35RTTk8DCqhG7A3jiudUqj'},
    {'consumer_key': 'ZMcJlgB2POlzVbqQVgwBfMes5',
     'consumer_secret': 'tL3Ch94uEORN7dZrrWEoKZdjVPiMYscE0Am5a1MqUyskFmXTwD',
     'access_token': '2575592904-DFVFcTEZ7bB4qVCAanC32jiGJtU5LP02eOuESFK',
     'access_secret': '0HsSuWz3MpKMqWYxSod77WVW5WXX10Trq13z5Nry348bN'},
    {'consumer_key': '2c56wTHKAS21gA4UAZzkkwzqZ',
     'consumer_secret': 'fxAfJHIBqHGwL0f42zlzUEtJUmsxyHAD8MQ7gEqYqaau7KZyxJ',
     'access_token': '2575592904-mQN46mJwpSMj9CW5jakd79c5T34x7DjBApSf4W5',
     'access_secret': 'bYFLbECe6EjNvKM6xm4BEzA6IZk00HZ5XtaFwJ4GxfhGp'},
    {'consumer_key': 'QVpJC7lLXt0VqSRiB4oWOxLux',
     'consumer_secret': 'r0rQgZku1qtZtdXV1z4luOh2yiWNPWGxhdfxhl0RPyqkASCJxc',
     'access_token': '2575592904-rjouP5t0EJ1iu7vFM3kU5DBKLAZHA94YzSvtau6',
     'access_secret': 'gql0ekQiHWx5rQmurJKFp9EiNrvl6MJ7mF04rqFBwGGYx'},
    # amyxie1994
    {'consumer_key': 'gbqEmrklOoqFEpvEFfYFmufqF',
     'consumer_secret': 'QgzOCavAiPh1wWrSAPBvj7ZZQyMJ9KxUp4Qz8SSh1WzgblQ7oE',
     'access_token': '853486226443194368-M6NEQ1DGRZX8eUUKmaXj9CsY3vv7gLA',
     'access_secret': '8qxOum5J3Uuc4kbpnoNykBoDQcHSzTF4xW9JRWhZwIbdh'},
    # nanjiangl
    {'consumer_key': 'pXWJNLIVTT8NNmO5X2x7nQf2P',
     'consumer_secret': 'RETU3DeIbqq111XWBByJppmOOZHDNnMsb4ergE2ZQZlUDVeFCY',
     'access_token': '851662491927040000-df4TR9seEcvDWGva4OcNjMrPAC0z0ik',
     'access_secret': '0zCj63JPIcx28g1UcquZKv3vnTGXHJFaO60sPgOQgbmHY'},
    {'consumer_key': 'oqUpEOmJwT63m2ifD45EwWgOJ',
     'consumer_secret': 'Qd3hcSkzAATYZQpv9z5AjvOkOIqdN6fvOU1JNlGB1BsgJ0j3zp',
     'access_token': '851662491927040000-7gLy5OGe00h8evy5jKKBSNOanAVZeFn',
     'access_secret': 'd5BjxJ5EnYZG02bBpPmwpB2fF7ZuWoUzH96xmhoVef6JX'},
    {'consumer_key': 'D1CUDFJ8aLZ9D8VGy6vcleYfP',
     'consumer_secret': '12TndZwTsN5YReB2YJx0xIPmSvUlr7HbdBbWasrqZwA8Bks76z',
     'access_token': '851662491927040000-RLTayq8a5pzT0zz337skagbe36osxbd',
     'access_secret': 'eZ2pcRvumBQ5eO9E96VFM3XLLacf0dVwJEeBrJdSpLykR'},
    {'consumer_key': 'Z54Zw9U1v5HwE63UJgio1C2Eh',
     'consumer_secret': 'HebEb4OereuW3VKTSByShOIV10N8hgadyrMXc3KSywJDoCGv1V',
     'access_token': '851662491927040000-VNCzPjxMMU3wSxEC1CbshQJVkh1ie8Q',
     'access_secret': 'hmtQPook0vBXQzzpgmaAvcWqzyRtXQhctaVgk61YBboIP'},
    {'consumer_key': 'g5pLp0KLQ5hgRcR3fxTxQsd5p',
     'consumer_secret': 'RtgxcATM7I5ELmEnLycjI1JUBI6GY7BQF6xP2BIPY5YkvfSU40',
     'access_token': '854862643948957696-iRKez7j7mjH32YNqiKi36eotm2U2qEv',
     'access_secret': 'cGdFdOwrVHOKP37GGzqaD6SuElsb7D7Fa1nXYkGpH9DcK'},
    {'consumer_key': 'ybqdx2QUlaXGfuPtvq5xDum5s',
     'consumer_secret': 'XCmegsIkx5DhFEsfdukpwVLTcvCeObG6PVx073pYGFAiN5tvwd',
     'access_token': '854862643948957696-sJezdcXBvXLTqBM3V7ltJ7EX99v76Ve',
     'access_secret': 'ATo8pboqahtHE3AnGqeDd9bOGLoXtiQEcjpAawdORB4AV'},
    {'consumer_key': 'RbMGm2WJE1P7eQQJkYJwO8RuR',
     'consumer_secret': 'o2AwpUtXCM8mUDNmYTitCPBuqmXtkeWm7PANE1W2PN1Rmc0aaB',
     'access_token': '854862643948957696-N44ZODQraL0fpFFGCuP0wgDLKIcARCk',
     'access_secret': 'JfSi4XZ5fo0ZlxDfwuxs4cSDQnlaZQ0yS4VYOWEvHaFqc'},
    {'consumer_key': 'z0OFMGNhUeNyr3BLJUzP9nQ6t',
     'consumer_secret': 'D5rxzXStJZVo8UxNCu707lljqwjytsJTr4ZEIAEgYEZMcrU5Rh',
     'access_token': '854891237358186496-Ul8AHeMDRgyjrKXSdt4PD40Kkku693s',
     'access_secret': 'kWO9T8BZQBKMlrFUaXC2lDa8jD3f8dU7RNlXoxrpNoZ9Y'},
    {'consumer_key': '7rF55CFf3JWQNk5iEsBLIDqzM',
     'consumer_secret': 'EwdO46xaOrWXkh4raX0prEAWzBCHKOB2JDqhrALcvSrIFSJQRy',
     'access_token': '854891237358186496-7WkKebZtQbitwSHRw3nZYEoxjzgVE0O',
     'access_secret': 'nY9gniL3jUSv5YQUv5l4Sy8MS0Iufyj8UJqKkCoAjnNds'},
    {'consumer_key': 'ZppJmYBBlRz0H50G3UIk2cfy2',
     'consumer_secret': 'GX0rDbhcgsXb6qlvAnWIjLung3G8ELPfzdhrs4kU2EJiNMuX58',
     'access_token': '854891237358186496-ti1HW24Xk49vG2S8xvmES8oGvI8xBQR',
     'access_secret': 'A3f1AYO4vYOgfaSywTtGgksZybBhdtXA1QMbmB5DQfmvt'},
    # haozhi
    {'consumer_key': 'EuRzEuEEElgI3cVbu7qRz5l5v',
     'consumer_secret': 'YlZmxZC4kXH3pZXu3Zi94xa876cOnekZVWPiWpIknb0DUXxWMz',
     'access_token': '978447119169105920-iS2pxiVlnlzAqmyCzXxIyixgdThO90S',
     'access_secret': 'LqqXDAf0ZDamKfC2EwoHeUBSVCJmUHcEH7Rb6NSLrodmu'},
    {'consumer_key': 'UOA0OMsldrPosUXUKQuVdqXBT',
     'consumer_secret': 'p4ph5xMChEPOZKWZJxOr3c7ltJFOAcgJYQBaScD6vH0yaygkvY',
     'access_token': '978447119169105920-IV7rrq3gnlDfJTtxkv7SqYOZvk8gFBE',
     'access_secret': 'ILZfVoN3bNvKMAewpfQyRAOZxEeBCUGUEReDhrm7BuwI7'},
    {'consumer_key': 'W9AL1hTQKvDTDQelzXBZPKmnX',
     'consumer_secret': '8rJ5OXbELOg0dN3GZAJnAMmdr1fE5LyZKocuMLN1K4WQiKunFl',
     'access_token': '978447119169105920-NCY44mPGEW1VxPvAZWHWmXSztAa2L3z',
     'access_secret': 'qPwedtQaxiob16F5JyH4CaqXdVbysElYeZpbXl4BkaZvz'},
    {'consumer_key': 'lpuHG48yc9miriNsKESW4IMov',
     'consumer_secret': 'JveglsIK7LCJVdZ4duJsJuf8LLzZcDjNHaB1jDWH2jNp1zaveM',
     'access_token': '978447119169105920-p8h6uhpQGnOGlDBbdnCoI1fO9VlVPUl',
     'access_secret': 'VQ7uGXu4gkqrWMxHEASKFoLgq5iwPVBgmZYOMvUoSiXDM'},
    {'consumer_key': 'U9DvHj4hD7lRH6ILmpjC7ljJJ',
     'consumer_secret': 'AsPWk6K3IGJ4wkHfs7VKxkVy11NVDtIxqfQ7yoK6lDcA42QXbU',
     'access_token': '935680283596152832-KUHA6L4UVKfYTZnGuyHSwMd61FCZgQ2',
     'access_secret': 'zNWyOBnKr7BULGRSvdun8Bd8DXx2REQAq7ORatLYjTFCd'},
    {'consumer_key': 'hIJolhopD8esExslpswYrROid',
     'consumer_secret': 'mSZsybM5K880aag0fyChpz7ti4O8Iym6CgBci8A3oKVsNOkQhZ',
     'access_token': '935680283596152832-q78VqOhmt1bEA2vbp59vOnkH0Hx8h7u',
     'access_secret': '3QWVu7EikAGyR7k1jxhgx8v7fSW9dfnrAlz5gAoxnEuba'},
    {'consumer_key': 'yW6Vx2NRP0NOg3VQzK01uZEE4',
     'consumer_secret': 'yMZKpQnE3FstebPvBXo1EALQzvFzLSGV6VWBb4Xpbctt8bVcEC',
     'access_token': '935680283596152832-7JCHL1d3QIYycJGKUcQW2z7IksXPjQ2',
     'access_secret': 'shP4UUmQRydGdo9fma0cWO08QnMbi3gWu1joYkpvMLXhi'},
    {'consumer_key': 'DEoGNcZhPdl64W4Buec4xQgpS',
     'consumer_secret': 'GwjloNzFTWI6FhuRpVciVbTn5tZPw5Z7W48vVytXyigeF43yah',
     'access_token': '989455040665477120-4WTiVwFEu7zvG9adym6FfbWsFu9atCA',
     'access_secret': 'c92I98aKhMFgt0z3jf6399TTM2x2ERRz7FlFgRt8JOWmk'},
    {'consumer_key': 'RayLQCoRZt98AL4mr5RmYiIqP',
     'consumer_secret': 'JPeF5Z1WojnOFHdmvSaKuNmjlQ0E55sX49BGnT5OyZDKPggt3O',
     'access_token': '989455040665477120-tHM8mGgbTCPk3Vx4riisPVMXD7pqAVA',
     'access_secret': '7jvixJcqOJH0svavyapJ8glglZwe2kpbtHwETYOJHsCob'},
    {'consumer_key': 'FiijCOXn7939kVBm8NRcgTywy',
     'consumer_secret': 'mAd7XentqnRAWX1EyB5TszeB2MSyU0QOuBEw2ypN8LZMPPeBkq',
     'access_token': '989455040665477120-ngsmkYktwB0Jo301Lv672cTegJ3mAch',
     'access_secret': 'pdmOdpuJXlW1hQMUQuUVjvAfAfcpPbTOBqheHgWfr1kgP'},
    {'consumer_key': 'Y6cuNl91bWRFZzableg3HPrVA',
     'consumer_secret': 'pIBsfPcWSxdf8zAnVkd16uPpokVckj9TGWejyyApkegYUAACVe',
     'access_token': '989455638324391936-juE0JR6DN4S8qm5UixuTrhrsTxFLs5a',
     'access_secret': 'Ar5RuApQpX3ZB1dDtTKXmLo4zdodmuwSK2uP6rIj86DFR'},
    {'consumer_key': 'qwCPYDOFo6qqe6Xpu2clXPvXh',
     'consumer_secret': 'x0HnBOIZGMPJe8Pr0W1AN7LPJkOY8NmpQuzeTspudjJf6o83i2',
     'access_token': '989455638324391936-58CXe7ZBUsqI5NEIbM4c5MhNo7IeReI',
     'access_secret': 'Y65KlpFY2bhhL27ADVkm31wFI1McnOdfxNZWQdltNymrC'},
    {'consumer_key': 'vfHk19RPkwuxgJZGAPIgTLoM6',
     'consumer_secret': 'kjO4TnfafzetdUntuPrmdng9JiTY7oA8NxBzFFebCCLH5KRnDJ',
     'access_token': '989455638324391936-bGbvc5pIHQzJc3ajPE2VTtDqrxlqWIh',
     'access_secret': 'CVZJ1XK9PQWMQzpgAmrkVUj4Aifj4ptG0Ygw8ALdcRI7H'},
    {'consumer_key': 'YVDQHxZSiJ3cFQ6kUcLPLCyFI',
     'consumer_secret': 'xEjvJAy5plmr1kBF17pxhFG7jtuVnPW8AvkzGuDbRnwtWlJoNx',
     'access_token': '989453544209055744-h6Tq0Pg7Qbn1HQxgSyGbq17LJMb9oOl',
     'access_secret': 'wVkmMcPXqwxCn82vn8pIknHn28mpNjkLSgbzGIpNYDDzg'},
    {'consumer_key': 'XSJmVLzUNbzzaLfervuKoNVkm',
     'consumer_secret': 'oMxCQW5tTjrF7DsxdYtzVXOe5JGT367gpNWZf4w8p1zJTto43w',
     'access_token': '989453544209055744-sh1NkTwmRR7nwFt8nRW1OCp2ypvWHy9',
     'access_secret': 'oi5352LPaUxFNFRzQXaxPT0WpCqj283NBDq0lZv6iQsHl'},
    {'consumer_key': 'xKVt91wj4KEV8OMmFeX9kpjdb',
     'consumer_secret': 'DVfLORZPgxYv2AaZjL5OwG01LQeBWMs2ouXj7viK5eRiKSyMMA',
     'access_token': '989453544209055744-fcvjVyYqEwsv1kSkpxewfwJ1CUIGk2D',
     'access_secret': 'N3FilAfaKgqt2de2Y8n4WAvRsFlJobxfbQ5exY8mGsIi1'},
    {'consumer_key': 'i7jvKKyWlh7RyQeVsTo0vKAwM',
     'consumer_secret': 'cPqd7o0KlhKucGv2i8Bdjvxrz35iXLLPslSCkineXEMLX5GK3c',
     'access_token': '989467485853249536-zbXNeDgcGycqgWogQZ8AWES2XRY4viQ',
     'access_secret': 'ctgN82fFeCxZxvwUTrCAiT9uPdpBhblN0vP3PUWlP8Qop'},
    {'consumer_key': 'cDTYOHmBJiBOhPfzOfJVOSNTe',
     'consumer_secret': 'ZXnqhZy0NeWvU3i3bvh44T29SHrj8Qd1K2JQAd36r1B1iGZAbL',
     'access_token': '989467485853249536-d8hyWyraLQF6ekWQte2OL9jiH9vTY66',
     'access_secret': 'hXNayIxvAbcBldDZZa8LlnxnnTRODRzfFrP6AGUroqNjn'},
    {'consumer_key': 'A7TnZyUndm68dwbKamb6lYPQx',
     'consumer_secret': 'ciEcb8g5D4buY1NIIM8OPoYxTBxkrvwHw9UAsbK8xlL9w5k4IF',
     'access_token': '989467485853249536-PJUCS89mB8MQF1UCOZF40ITXFG0Toor',
     'access_secret': 'HL6SWnSnVmRcPjT0AXKEFmJC5MJrWqjVbMdxl9Pk5ccKy'},
    {'consumer_key': 'VQz0cSOQ6v9wmaTxsJB6zdVsF',
     'consumer_secret': 'bPFWR1rHlgvcGd0UXyxmkRxPvy366bsW9ceA627DUme4zF1YT5',
     'access_token': '989469942423547904-cDVat288bQmDg6sPc5LxUSAPWOjXLDe',
     'access_secret': 'fetZDLR33Y6SAW8Cd1qJ2dcuxcqIf5Hs5P6i1kKey0Fbx'}
]

# add accounts to apis
apis = []
for account in accounts:
    auth = OAuthHandler(account['consumer_key'], account['consumer_secret'])
    auth.set_access_token(account['access_token'], account['access_secret'])
    apis.append(tweepy.API(auth))
l_apis = len(apis)

tlog('Api appended to list.')

couch = couchdb.Server('http://' + couchIP + ':5984')
db = None
try:
    db = couch[db_twitter]
except couchdb.http.ResourceNotFound as e:
    try:
        couch.create(db_twitter)
        db = couch[db_twitter]
    except couchdb.http.PreconditionFailed as e:
        pass

db_config = couch[db_config_name]
while True:
    try:
        doc_config = db_config[doc_config_name]
        thread_rank = doc_config['rank']
        doc_config['rank'] += 1
        db_config.save(doc_config)
        is_stream = True if thread_rank < 4 else False
        acc_pos = thread_rank
        break
    except couchdb.http.ResourceConflict as e:
        tlog((e, type(e)))

tlog(('Thread rank:', thread_rank, 'is stream:', is_stream, 'acc_pos', acc_pos))


def out_error(e, add_info=None):
    with open('error.out', 'a') as f:
        if add_info is not None:
            f.write('add info: ' + str(add_info) + '\n')
        f.write(str(e) + ', ' + str(type(e)) + '\n')
        f.write(traceback.format_exc())
        f.write('\n########################################\n')


def out_cluster():
    with open('cluster.out', 'a', encoding='utf8') as f:
        while not un_user.empty():
            screen_name = un_user.get()
            f.write(screen_name)
            f.write('\n')
        f.write(str(cnt))
        f.write('\n########################################\n')


def screen_name_in_db(screen_name):
    try:
        if screen_name[0] == '_':
            screen_name = 'underscore' + screen_name
        _ = db[screen_name]
        return True
    except couchdb.http.ResourceNotFound:
        return False
    except Exception as e:
        out_error(e, screen_name)


def in_locations(location):
    la = location
    for lb in locations:
        if lb[1] <= la[0] <= lb[3] and lb[0] <= la[1] <= lb[2]:
            return True
    return False


def in_city(friend):
    if len([True for city in place_list if city in friend.location.lower()]) != 0:
        return True
    try:
        if len([True for city in place_list if city in friend.time_zone.lower()]) != 0:
            return True
    except AttributeError:
        pass
    try:
        if len([True for city in place_list if city in friend.status.place.lower()]) != 0:
            return True
    except AttributeError:
        pass
    return False


def save_db(screen_name, geo_list):
    tc_count = len(geo_list)
    geo_list = [geo['coordinates'] for geo in geo_list if
                geo is not None and in_locations(geo['coordinates'])]
    tcg_count = len(geo_list)
    try:
        if screen_name[0] == '_':
            screen_name = 'underscore' + screen_name
        db.save({'_id': screen_name, 'geos': geo_list})
        cnt[tc] += tc_count
        cnt[tcg] += tcg_count
        cnt[uc] += 1
        if len(geo_list) != 0:
            cnt[ucg] += 1
    except couchdb.http.ResourceConflict as e:
        tlog(('ap', acc_pos, screen_name, type(e), e))
    except ConnectionResetError as e:
        tlog(('ap', acc_pos, screen_name, type(e), e))
    except couchdb.http.ServerError as e:
        tlog(('ap', acc_pos, screen_name, type(e), e))


def friends(screen_name):
    f_c = min(max_friend_count, un_user_capacity - un_user.size())
    if f_c >= 10:
        for friend in tweepy.Cursor(apis[acc_pos].friends, screen_name=screen_name).items(f_c):
            if screen_name_in_db(friend.screen_name) or friend.followers_count < 5 or friend.statuses_count < 10:
                continue
            if not in_city(friend):
                continue
            un_user.put(friend.screen_name)


def get_tweets_geos(screen_name):
    tweets = tweepy.Cursor(apis[acc_pos].user_timeline, screen_name=screen_name, include_rts=False).items(
        max_status_count)
    geo_list = [status.geo for status in tweets]
    return geo_list


def tweets(screen_name):
    if screen_name_in_db(screen_name):
        return
    geo_list = get_tweets_geos(screen_name)
    save_db(screen_name, geo_list)
    tlog(('tc', cnt[tc], 'tcg', cnt[tcg], 'uc', cnt[uc], 'ucg', cnt[ucg], 'uq', un_user.size(), screen_name))


def next_acc_pos():
    global acc_pos
    acc_pos = (acc_pos + 4) % l_apis


def twitter_api(func, screen_name):
    global acc_pos
    while True:
        try:
            func(screen_name)
            break
        except tweepy.error.RateLimitError as e:
            next_acc_pos()
            tlog(('ap', acc_pos, screen_name, type(e), e))
            sleep(5)
        except tweepy.error.TweepError as e:
            tlog(('ap', acc_pos, screen_name, type(e), e))
            if e.response is None:
                next_acc_pos()
                traceback.print_exc()
                out_error(e)
                break
            elif e.response.status_code in [326, 429, 500, 503]:
                # 326 account banned
                # 429 too many requests
                # 500 Internal Server Error
                # 503 Service Unavailable
                next_acc_pos()
                sleep(5)
            elif e.response.status_code in [401]:
                # 401 private tweet account
                save_db(screen_name, [])
                break
            elif e.response.status_code in [404]:
                # 404 Not Found
                next_acc_pos()
                break
            else:
                next_acc_pos()
                traceback.print_exc()
                out_error(e)
                break
        except Exception as e:
            next_acc_pos()
            tlog(('ap', acc_pos, screen_name, type(e), e))
            traceback.print_exc()
            sleep(5)
            out_error(e)
            out_cluster()
    pass


if is_stream:

    class MyStreamListener(tweepy.StreamListener):

        def on_status(self, status):
            if screen_name_in_db(screen_name):
                return
            geo_list = get_tweets_geos(screen_name)
            save_db(status.user.screen_name, geo_list)
            tlog(('tc', cnt[tc], 'tcg', cnt[tcg], 'uc', cnt[uc], 'ucg', cnt[ucg], status.user.screen_name))


    location = locations[thread_rank]
    tlog(location)
    while True:
        try:
            myStreamListener = MyStreamListener()
            myStream = tweepy.Stream(apis[acc_pos].auth, myStreamListener)
            myStream.filter(locations=location)
        except Exception as e:
            out_error(e)
            sleep(5)
            acc_pos = (acc_pos + 4) % l_apis
            tlog(('ap', acc_pos, str(e)))
            traceback.print_exc()
else:
    un_user = OrderedSet()
    try:
        rank_ = 2 * (thread_rank - 4)
        for i in range(2):
            un_user.put(initial_screen_name[rank_ + i])
        while True:
            screen_name = un_user.get()
            twitter_api(friends, screen_name)
            twitter_api(tweets, screen_name)
    except KeyboardInterrupt as e:
        out_error(e)
        out_cluster()
        tlog(('ap', acc_pos, type(e), e))
        traceback.print_exc()
    except Exception as e:
        out_error(e)
        out_cluster()
        tlog(('ap', acc_pos, type(e), e))
        traceback.print_exc()

# TGF174

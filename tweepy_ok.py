# -*- coding: utf-8 -*
import tweepy
from textblob import TextBlob
import oandapyV20 as oandapy
import oandapyV20.endpoints.accounts as accounts
from tools import Utility
import matplotlib
import numpy as np
import pandas as pd

consumer_key = "m2rSkYpqbjXMTKZw2wznAUWXk"
consumer_secret = "Prh1PCRRSurTBDKPqaUGZvKxIEqlB1rDTgAyLblNkd85To6alJ"

access_token = "731414015147163648-KM4O1QkC5HxSldRCGJ71QrPfBhcLUeS"
access_token_secret = "w11yNKclWFIoNXE32sfwDNieOnubCttcmViMHKSusbaDY"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)
oanda = oandapy.API(environment="practice",
                    access_token=Utility.getAccountToken(),
                    headers={'Accept-Datetime-Format': 'UNIX'})


def create_instrument_list():
    oanda = oandapy.API(environment="practice",
                        access_token=Utility.getAccountToken(),
                        headers={'Accept-Datetime-Format': 'UNIX'})
    r = accounts.AccountInstruments(accountID=Utility.getAccountID())
    name_list = []
    for instru in oanda.request(r)["instruments"]:
        name = instru["name"]
        index_underscore = name.find("_")
        name_front = name[:index_underscore]
        name_back = name[(index_underscore+1):]
        name_list.append(name_front)
        name_list.append(name_back)

    name_list = set(name_list[:5])
    return name_list


name_list = create_instrument_list()
print("List of asset names contains {} items.".format(len(name_list)))
i = 0
for name in name_list:
    print("\t {}".format(i))
    for tweet in api.search(q=name, rpp=100, count=200):
        i = i + 1

#!/usr/bin/env python
# coding: utf-8

import os
import tweepy
from datetime import datetime

from utils import *

from creds import *


OUTPUT_FOLDER = '/home/omilab/sho/twitter_getter/trending_topics'

def rules(l):
    if l['placeType']['name']=='Country':
        return True
    if l['country']=='Israel':
        return True
    if l['name']=='Worldwide':
        return True
    return False

def get_trends_by_locs(api, locs):
    all_trends = []
    for loc in locs:
        tr = {'location': loc}
        timestamp = datetime.now()
        tr['timestamp'] = timestamp.isoformat()
        tr['response'] = api.trends_place((loc['woeid']))
        ts_clean = timestamp.strftime('%Y%m%d%H%M%S')

        all_trends.append(tr)
        
    return all_trends
    
if '__main__'==__name__:
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

    #auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth,wait_on_rate_limit=True)
    
    available_locs = api.trends_available()
    use_locs = [l for l in available_locs if rules(l)]
    
    timestamp = datetime.now()
    date = timestamp.strftime('%Y-%m-%d')
    output_folder = os.path.join(OUTPUT_FOLDER, date)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        
    ts_clean = timestamp.strftime('%Y%m%d%H%M%S')
    jsonl_path = os.path.join(output_folder, ts_clean+'.jsonl.gz')

    all_trends = get_trends_by_locs(api, use_locs)
    
    writeall_jsonl_gz(jsonl_path, all_trends)

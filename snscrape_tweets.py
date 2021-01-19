#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
from datetime import datetime
import time
import sys
import gzip
import shutil
import subprocess

from utils import *

#from creds import *

OUTPUT_FOLDER = '/datadrive/twitter_getter/scrape_tweets'
QUERY_XLSX = '/home/omilab/sho/twitter_getter/query_design/english_queries_v1.xlsx'

WAIT_TIME = 0.1 #seconds

def escape_query(q):
    q = q.replace('"', '\\"')
    return q

def get_queries(query_path):
    qdf = pd.read_excel(query_path)
    all_queries = list(zip(qdf.ngram.tolist(), 
                       qdf.syntax.apply(escape_query).tolist()))
    return all_queries

def run_queries(queries, output_folder, date_range):
    for ngram, query in queries:
        slug = slugify(query).replace('_','-')
        ngram_slug = slugify(ngram).replace('_','-')

        for cd in date_range:
            nd = cd+pd.Timedelta(1, unit='d')
            cd = cd.strftime('%Y-%m-%d')
            nd = nd.strftime('%Y-%m-%d')
            date_folder = os.path.join(output_folder, cd, ngram_slug)
            if not os.path.exists(date_folder):
                os.makedirs(date_folder)
            file_name = f'{cd}_{slug}.jsonl'
            output_path = os.path.join(date_folder, file_name)
            if not os.path.exists(output_path):
                sns_cmd = f'snscrape --jsonl --since {cd} twitter-search "{query} until:{nd}" > {output_path}' 
                print(sns_cmd)
                os.system(sns_cmd)
                #subprocess.Popen(sns_cmd, shell=True, executable='/bin/bash')
                time.sleep(WAIT_TIME)
        
def compress_results(output_folder):     
    for folder in os.scandir(output_folder):
        if '.ipynb' in folder.name:
            continue
        for file in os.scandir(folder):
            if not file.path.endswith('.jsonl'):
                continue
            compressed_path = file.path+'.gz'
            if not os.path.exists(compressed_path):
                with open(file.path, 'rb') as f_in:
                    with gzip.open(compressed_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            os.remove(file.path)
            
if '__main__'==__name__:
    
    if len(sys.argv)==1:
        yesterday = (datetime.now()-pd.Timedelta(1, unit='d'))
        date_range = [yesterday, yesterday]
    elif len(sys.argv)==3:
        date_range = pd.date_range(sys.argv[1], sys.argv[2]).tolist()
    else:
        print('Bad params. None or two dates please.')
        sys.exit()            
    
    print (f'--- date_range = {date_range}')
    queries = get_queries(QUERY_XLSX)
    print (f'--- Got {len(queries)} queries from {QUERY_XLSX}')
    print ('--- Running queries...')
    run_queries(queries, OUTPUT_FOLDER, date_range)
    print('--- Compressing results...')
    for date in date_range:
        compress_results(os.path.join(OUTPUT_FOLDER, date.strftime('%Y-%m-%d')))
    print('--- Done.')

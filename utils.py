#!/usr/bin/env python
# coding: utf-8

import gzip
from typing import List, Dict
import jsonlines
import unicodedata
import re

def writeall_jsonl_gz(filename, payload: List[Dict], dumps=None):
    with gzip.open(filename, 'wb') as fp:
        json_writer = jsonlines.Writer(fp, dumps=dumps)
        json_writer.write_all(payload)


def read_jsonl_gz(filename) -> List[Dict]:
    data = []
    with gzip.open(filename, 'rb') as fp:
        j_reader = jsonlines.Reader(fp)

        for obj in j_reader:
            data.append(obj)

    return data
                
def slugify(value, allow_unicode=False):
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')



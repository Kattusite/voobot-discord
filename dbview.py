#!/usr/bin/env python -i

""" Run with python -i to drop into a REPL to interact with the cache db. """

import tinydb
from os import path as op

CACHE_DIR='cache'

db = tinydb.TinyDB(op.join(CACHE_DIR,'cache.json'),
                           encoding='utf-8',
                           indent=2,
                           ensure_ascii=False)

channels = db.table('channels')
cache = db.table('message_cache')

Msg = tinydb.Query()

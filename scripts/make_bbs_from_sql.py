import matplotlib.pyplot as plt

import sqlite3 as lite

import sys; sys.path.append('../')
import gcmt_utils.sql_utils as sq
import gcmt_utils as gc


gcmt_db = '../data/gcmt_table.sqlite'
gcmt_table = 'GCMT_events'


con = lite.connect(gcmt_db)

con.row_factory = lite.Row
cur = con.cursor()
cur.execute('SELECT * from {}'.format(gcmt_table))
events = cur.fetchall()
for i, event in enumerate(events):
    gc.make_beachball(event, 
                      #directory='/mnt/data/gcmt/bbs/svg/',
                      directory='../data/bbs/svg/',
                      fig_format='svg', bb_size=20, bb_width=100, dpi=15)
    #gc.resize_bb_file(event, directory='../data/bbs')

    if i % 1000 == 0:
        print(i)


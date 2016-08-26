import json
import pandas as pd
import subprocess
import time
import sqlite3 as sq

gcmtdir = '../../'

#gcmt_json_f = '{}/globalcmt/geojson/gcmt_4_jan_2015.geojson'.format(gcmtdir)
gcmt_json_f = '/Users/itchy/research/seis/gcmt/gcmt_utils/data/gcmt_min_zoom.geojson'
bb_path = '{}/beachballs/png/'.format(gcmtdir)
bb_r_path = '{}/beachballs/png_reduced/'.format(gcmtdir)

#with open(gcmt_json_f, 'r') as f:
#    dd = json.load(f)
gcmt_db = '../data/gcmt_table_bb_urls.sqlite'
gcmt_table = "GCMT_events"

with sq.connect(gcmt_db) as con:
        gcmt_df = pd.read_sql_query("SELECT * FROM {}".format(gcmt_table), con)


t0 = time.time()

for i in gcmt_df.index:
    fsize = int(0.7 * gcmt_df.ix[i, 'Mw']**2.1) # scale image
    event = gcmt_df.ix[i, 'Event']

    subprocess.call(
      'convert {bb}{n}.png -trim -resize {s}x{s} {bbr}{n}.png'.format( **{
                                                'bb':bb_path, 'n':event,
                                                's':fsize, 'bbr':bb_r_path}),
      shell=True)

    if i % 5000 == 0:
        print(i)

t1 = time.time()

print('done in {} s'.format(t1-t0))




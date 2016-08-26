import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import json
import sqlite3 as sq
#from mpl_toolkits.basemap import Basemap
import time
#from scipy.stats import binned_statistic_2d
#from matplotlib.colors import LogNorm

gcmt_sql = '../data/gcmt_table_bb_urls.sqlite'
gcmt_geojson = '../data/gcmt_min_zoom.geojson'

#with sq.connect(gcmt_sql) as con:
#    eqs = pd.read_sql_query(
#        "SELECT Latitude, Longitude, Depth, Mw from GCMT_events", con)

print('loading data')
with open(gcmt_geojson, 'r') as f:
    gj = json.load(f)

feats = gj['features']

def row_from_feat(feat, name):
    return pd.DataFrame({'Longitude' : feat['geometry']['coordinates'][0],
                         'Latitude' : feat['geometry']['coordinates'][1],
                         'Mw' : feat['properties']['Mw']},
                        index=[name])

eqs = pd.concat([row_from_feat(f, i) for i, f in enumerate(feats)])


spacing = 1

lon_edges = np.linspace(-180,180, (360 / spacing) +1)
lat_edges = np.linspace(-90,90, (180 / spacing) +1)

#print('calculating earthquake density')
print('calculating min zoom')
t0 = time.time()

#D, x_edges, y_edges = np.histogram2d(eqs.Longitude, eqs.Latitude,
#                                     bins=[lon_edges, lat_edges],
#                                     )

lon_idxs = np.searchsorted(lon_edges, eqs.Longitude) -1
lat_idxs = np.searchsorted(lat_edges, eqs.Latitude) -1

ll_idxs = tuple(zip(lon_idxs, lat_idxs))

#pt_dens = np.array([D[ll_idx] for ll_idx in ll_idxs])
#pt_dens[pt_dens == 0] = 1

#eqs['pt_dens'] = pt_dens
#

eqs['ll_tup'] = ll_idxs

def min_dens(zoom, scale=1.5):
    if zoom < 5:
        return 1
    else:
        return int(2 **((zoom - 5) * scale))

zoom_thresholds = np.array([[min_dens(e), e] for e in np.arange(1,12)])

def min_zoom(vals, zoom_threshold=zoom_thresholds):
    ranks = len(vals) - vals.argsort().argsort()
    
    return zoom_threshold[np.searchsorted(zoom_threshold[:,0], ranks),1]

eqs['min_zoom'] = eqs.groupby('ll_tup')['Mw'].transform(min_zoom)
eq_mzs = eqs.min_zoom.values

t1 = time.time()
print('done in {:.1f} s at {} deg spacing'.format((t1-t0), spacing))
print('inserting rank in features')

def insert_min_zoom(i, eq_mz):
    feats[i]['properties']['minZoom'] = eq_mz

_ = [insert_min_zoom(i, eq_mz) for i, eq_mz in enumerate(eq_mzs)]

print('done')

with open(gcmt_geojson, 'w') as f:
    json.dump(gj, f)

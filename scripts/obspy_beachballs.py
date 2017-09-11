from obspy.imaging.beachball import beach, beachball
import sqlite3 as sq
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import time

gcmt_db = '../data/gcmt_table.sqlite'
gcmt_table = "GCMT_events"

with sq.connect(gcmt_db) as con:
    gcmt_df = pd.read_sql_query("SELECT * FROM {}".format(gcmt_table), con)

    print(gcmt_df.shape)

def save_event_beachball(idx, df, bb_type='tensor', fig_format='svg',
                         directory='./', use_dir_format=True, bb_linewidth=2,
                         bb_size=20, bb_width=100, bb_color='b'):
    
    fig = plt.figure(1)
    
    if bb_type == 'double_couple':
        mt = df.ix[idx][['Strike_1', 'Dip_1', 'Rake_1']]
    elif bb_type == 'tensor':
        mt = df.ix[idx][['Mrr', 'Mtt', 'Mpp', 'Mrt', 'Mrp', 'Mtp']]

    event = df.ix[i, 'Event']

    beachball(mt, linewidth=bb_linewidth, size=bb_size, width=bb_width,
              outfile='{}/{}.{}'.format(directory, event, fig_format),
              fig=fig, facecolor=bb_color)

    plt.close(fig)


#bb_dir = '../data/new_bbs/svg'
bb_dir = '../../beachballs/png'

depth_min = 10. #gcmt_df.Depth.min()
depth_max = 700. #gcmt_df.Depth.max()

def depth_to_color(val, v_min=depth_min, v_max=depth_max, cmap='viridis',
                   log=True):

    colormap = plt.get_cmap(cmap)

    if log == True:
        v_min_ = np.log(v_min)
        v_max_ = np.log(v_max)
        val_ = np.log(val)

    else:
        v_min_ = v_min
        v_max_ = v_max
        val_ = val

    return colormap( (val_ - v_min_) / (v_max_ - v_min_))


depth_colors = depth_to_color(gcmt_df.Depth)

t0 = time.time()
for i in gcmt_df.index:
    try:
        save_event_beachball(i, gcmt_df, bb_type='tensor', 
                             bb_color=depth_colors[i],
                             directory=bb_dir,
                             fig_format='png')
    except:
        save_event_beachball(i, gcmt_df, bb_type='double_couple',
                             bb_color=depth_colors[i],
                             directory=bb_dir,
                             fig_format='png')
    if i % 5000 == 0:
        print(i)

t1 = time.time()

print('done with {} beachballs in {} s'.format(i+1, int(t1-t0)) )

import sys; sys.path.append('../gcmt_utils/MoPaD/')
import mopad as mp

'''
gcmt -> USE
r -> up
t -> south
p -> east


'''


def mt_params_from_eq_dict(eq_dict):

    e = eq_dict

    return (e['mrr'], e['mtt'], e['mpp'], e['mrt'], e['mrp'], e['mtp'])

def MT_from_eq_dict(eq_dict):

    return mp.MomentTensor(M=mt_params_from_eq_dict(eq_dict), in_system='USE')


def bb_from_eq_dict(eq_dict):

    mt = MT_from_eq_dict(eq_dict)

    return mp.BeachBall(MT=mt)





















test_eq_dict = {
 'Ms': 4.8,
 'Mw': 4.889210243986955,
 'body_wave_components': 24,
 'body_wave_shortest_period': 40.0,
 'body_wave_stations': 22,
 'centroid_date': '2015-01-01',
 'centroid_datetime': '2015-01-01 09:42:06',
 'centroid_depth': 29.4,
 'centroid_depth_err': 1.3,
 'centroid_lat_err': 0.04,
 'centroid_latitude': 55.07,
 'centroid_lon_err': 0.05,
 'centroid_longitude': 163.94,
 'centroid_time': '09:42:06',
 'centroid_time_err': 0.4,
 'cmt_code_version': 'V10',
 'cmt_event_name': 'C201501010942A',
 'cmt_timestamp': 'S-20150421120050',
 'cmt_type': 'CMT: 1',
 'depth_inv_type': 'FREE',
 'dip_1': 30.0,
 'dip_2': 61.0,
 'geog_location': 'OFF EAST COAST OF KAMCHA',
 'hypocenter_reference_catalog': 'PDEW',
 'mantle_wave_components': 0,
 'mantle_wave_shortest_period': 0.0,
 'mantle_wave_stations': 0,
 'mb': 0.0,
 'moment_rate_function': 'triangle',
 'moment_rate_half_duration': 0.6,
 'mpp': -1.61e+23,
 'mpp_err': 1.2e+22,
 'mrp': 8.99e+22,
 'mrp_err': 1.28e+22,
 'mrr': 2.03e+23,
 'mrr_err': 1.99e+22,
 'mrt': 8.89e+22,
 'mrt_err': 1.82e+22,
 'mt_exp': '23',
 'mtp': -9.02e+22,
 'mtp_err': 7.7e+21,
 'mtt': -4.18e+22,
 'mtt_err': 1.22e+22,
 'n': 2e+21,
 'n_azimuth': 213.0,
 'n_plunge': 7.0,
 'p': -2.429e+23,
 'p_azimuth': 121.0,
 'p_plunge': 15.0,
 'rake_1': 75.0,
 'rake_2': 99.0,
 'reference_date': '2015-01-01',
 'reference_datetime': '2015-01-01 09:42:00.700000',
 'reference_depth': 10.0,
 'reference_latitude': 55.29,
 'reference_longitude': 163.06,
 'reference_time': '09:42:00.7',
 'scalar_moment': 2.42e+23,
 'strike_1': 200.0,
 'strike_2': 37.0,
 'surface_wave_components': 84,
 'surface_wave_shortest_period': 50.0,
 'surface_wave_stations': 61,
 't': 2.411e+23,
 't_azimuth': 328.0,
 't_plunge': 73.0}



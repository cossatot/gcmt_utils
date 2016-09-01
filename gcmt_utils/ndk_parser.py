from ast import literal_eval as lev
from math import log10
import datetime as dt
import logging

#from .gcmt_utils import strip_dict, merge_dicts

'''
Functions to parse the Global CMT NDK format into more usable forms
'''

'''
NDK string parsing functions
'''

def parse_ndk_line_1(l1_string):
    l1 = {}
    l1['hypocenter_reference_catalog'] = l1_string[0:4]
    l1['reference_date'] = l1_string[5:15]
    l1['reference_time'] = l1_string[16:26]
    l1['reference_latitude'] = l1_string[27:33]
    l1['reference_longitude'] = l1_string[34:41]
    l1['reference_depth'] = l1_string[42:47]
    l1['magnitude'] = l1_string[48:55]
    l1['geog_location'] = l1_string[56:80]

    return l1
    

def parse_ndk_line_2(l2_string):
    l2 = {}
    l2['cmt_event_name'] = l2_string[0:16]
    l2['data_used'] = l2_string[17:61]         # need to parse further
    l2['cmt_type'] = l2_string[62:68]          # can be exanded
    l2['moment_rate_function_string'] = l2_string[69:80] # more parsing too

    return l2


def parse_ndk_line_3(l3_string):
    l3 = {}
    l3['centroid_params'] = l3_string[0:58]    # def needs more parsing
    l3['depth_inv_type'] = l3_string[59:63]
    l3['cmt_timestamp'] = l3_string[64:80]

    return l3


def parse_ndk_line_4(l4_string):
    l4 = {}
    l4['mt_exp'] = l4_string[0:2]
    l4['mt_params'] = l4_string[2:80]          # needs parsing
    
    return l4


def parse_ndk_line_5(l5_string):
    l5 = {}
    l5['cmt_code_version'] = l5_string[0:3]
    l5['mt_princ_axes'] = l5_string[3:48]
    l5['scalar_moment_string'] = l5_string[49:56]
    l5['fault_params'] = l5_string[57:81]

    return l5


'''string processing functions'''

def mb_ms_from_magnitude_string(mag_string):

    mag_string = mag_string.strip()

    mb = lev(mag_string.split()[0])
    Ms = lev(mag_string.split()[1])

    return {'mb' : mb, 'Ms' : Ms}


def parse_magnitude_string(eq_dict):
    try:
        mag_string = eq_dict.pop('magnitude')
        eq_dict.update(mb_ms_from_magnitude_string(mag_string))
    except KeyError:
        return


def parse_reference_locations_strings(eq_dict):
    
    params = ['reference_depth', 'reference_latitude', 'reference_longitude']

    for param in params:
        try:
            _par = eq_dict[param].strip()
        except KeyError:
            #logging.warning('EQ {} has no {}'.format(eq_dict['cmt_event_name'], param))
            logging.warning('EQ {} has no {}'.format(eq_dict['cmt_event_name'],
                                                     param))
        except AttributeError:
            pass
        try:
            par = lev(_par)
            eq_dict[param] = par
        except ValueError:
            eq_dict[param] = _par

    return
                

def parse_moment_tensor_params_string(eq_dict):
    try:
        mt_string = eq_dict.pop('mt_params')
    except KeyError:
        logging.warning('EQ {} has no moment tensor params'.format(
                                                    eq_dict['cmt_event_name']))
        return
    try:
        mt_exp = eq_dict['mt_exp']
    except KeyError:
        logging.warning('EQ {} has no moment exp'.format(
                                                    eq_dict['cmt_event_name']))
        return

    try:
        mt_dict = moment_tensor_params_from_string(mt_string, mt_exp)
    except IndexError:
        eq_dict['mt_params'] = mt_string
        return

    eq_dict.update(mt_dict)
    return


def moment_tensor_params_from_string(mt_string, mt_exp):
    mt_exp = str(mt_exp) # in case it's been made an int
    mt_list = mt_string.strip().split()

    mt_list = [lev(mt + 'e' + mt_exp) for mt in mt_list]

    d = {'mrr'     : mt_list[0],
         'mrr_err' : mt_list[1],
         'mtt'     : mt_list[2],
         'mtt_err' : mt_list[3],
         'mpp'     : mt_list[4],
         'mpp_err' : mt_list[5],
         'mrt'     : mt_list[6],
         'mrt_err' : mt_list[7],
         'mrp'     : mt_list[8],
         'mrp_err' : mt_list[9],
         'mtp'     : mt_list[10],
         'mtp_err' : mt_list[11]}

    return d


def parse_moment_tensor_axes_string(eq_dict):
    try:
        mx_string = eq_dict.pop('mt_princ_axes')
    except KeyError:
        logging.warning('EQ {} has no moment tensor axes params'.format(
                                                    eq_dict['cmt_event_name']))
        return
    try:
        mt_exp = eq_dict['mt_exp']
    except KeyError:
        logging.warning('EQ {} has no moment exp'.format(
                                                    eq_dict['cmt_event_name']))
        return

    try:
        mx_dict = moment_tensor_axes_from_string(mx_string, mt_exp)
    except IndexError:
        eq_dict['mt_princ_axes'] = mx_string
        return

    eq_dict.update(mx_dict)
    return


def moment_tensor_axes_from_string(mx_string, mt_exp):
    mt_exp = str(mt_exp) # in case it's been made an int
    mx_list = mx_string.strip().split()


    e0 = (float(mx_list[0] +'e' +mt_exp), float(mx_list[1]), float(mx_list[2]))
    e1 = (float(mx_list[3] +'e' +mt_exp), float(mx_list[4]), float(mx_list[5]))
    e2 = (float(mx_list[6] +'e' +mt_exp), float(mx_list[7]), float(mx_list[8]))

    eigs = sorted([e0, e1, e2], key=lambda x: x[0])

    d = {'p' : eigs[0][0], 
         'p_plunge' : eigs[0][1],
         'p_azimuth': eigs[0][2],
         'n' : eigs[1][0], 
         'n_plunge' : eigs[1][1],
         'n_azimuth': eigs[1][2],
         't' : eigs[2][0], 
         't_plunge' : eigs[2][1],
         't_azimuth': eigs[2][2]}

    return d
    

def parse_fault_params_string(eq_dict):
    try:
        fp_string = eq_dict.pop('fault_params')
    except KeyError:
        logging.warning('EQ {} has no fault parameters'.format(
                                                    eq_dict['cmt_event_name']))
        return

    try:
        fp_dict = fault_params_from_string(fp_string)
    except:
        eq_dict['fault_params'] = fp_string
        return

    eq_dict.update(fp_dict)


def fault_params_from_string(fp_string):
    fp_list = fp_string.strip().split()

    fp_list = list(map(float, fp_list))

    d = {'strike_1' : fp_list[0],
         'dip_1':     fp_list[1],
         'rake_1':    fp_list[2],
         'strike_2' : fp_list[3],
         'dip_2':     fp_list[4],
         'rake_2':    fp_list[5]}

    return d


def parse_scalar_moment_string(eq_dict):
    try:
        sm_string = eq_dict.pop('scalar_moment_string')
    except KeyError:
        logging.warning('EQ {} has no scalar moment string'.format(
                                                    eq_dict['cmt_event_name']))
        return
    try:
        mt_exp = eq_dict['mt_exp']
    except KeyError:
        logging.warning('EQ {} has no moment exp'.format(
                                                    eq_dict['cmt_event_name']))
        return

    eq_dict['scalar_moment'] = calc_scalar_moment(sm_string, mt_exp)
    return


def calc_scalar_moment(sm_string, mt_exp):
    mt_exp = str(mt_exp) # in case it's been made an int

    return lev(sm_string + 'e' + mt_exp)


def Mw_from_scalar_moment(scalar_moment):
    
    return (2/3.) * log10(scalar_moment) - 10.7


def add_Mw(eq_dict):
    try:
        eq_dict['Mw'] = Mw_from_scalar_moment(eq_dict['scalar_moment'])
    except KeyError:
        parse_scalar_moment_string(eq_dict)
    return


def format_ref_date_string(eq_dict):
    
    eq_dict['reference_date'] = eq_dict['reference_date'].replace('/','-')
    return


def get_date_from_ref_date(eq_dict):

    ref_date = dt.date(1,1,1)
    try:
        ref_date_string = eq_dict['reference_date']
    except KeyError:
        return ref_date

    ref_date_string = ref_date_string.replace('/','-') # may be done already

    try:
        ref_date=dt.date(*list(map(int, ref_date_string.split('-'))))
    except ValueError:
        return
    
    return ref_date


def get_time_from_ref_time(eq_dict):
    ref_time = dt.time(0)

    try:
        ref_time_string = eq_dict['reference_time']
    except (KeyError, AttributeError):
        return ref_time

    ref_time_list = ref_time_string.replace('.',':').split(':')
    
    try:
        for i in (0,1,2):
            ref_time_list[i] = int(ref_time_list[i])
        
        ref_time_list[3] = int( lev('0.'+ref_time_list[3]) * 1e6)

        if ref_time_list[2] == 60:
            ref_time_list[1] += 1
            ref_time_list[2] = 0

        ref_time = dt.time(*ref_time_list)

    except:
        return ref_time
    
    return ref_time


def get_ref_datetime(eq_dict):
    ref_date = get_date_from_ref_date(eq_dict)
    ref_time = get_time_from_ref_time(eq_dict)

    if ref_date == dt.date(1,1,1):
        logging.warning('EQ {} has a bad date string'.format(
                                                    eq_dict['cmt_event_name']))
    if ref_time == dt.time(0,0,0,0):
        logging.warning('EQ {} has a bad time string'.format(
                                                    eq_dict['cmt_event_name']))

    datetime = dt.datetime.combine(ref_date, ref_time)

    timestamp = datetime.isoformat(' ')

    eq_dict['reference_datetime'] = timestamp
    return 


def get_centroid_date_time(eq_dict):
    pass


def datetime_string_to_list(eq_dict):

    try:
        ref_datetime_str = eq_dict['reference_datetime']
    except KeyError:
        try:
            get_ref_datetime(eq_dict)
            ref_datetime_str = eq_dict['reference_datetime']
        except:
            logging.warning('EQ {} has a bad datetime'.format(
                                                    eq_dict['cmt_event_name']))
            ref_datetime_str = '1 1 1'

    dt_str= ref_datetime_str.replace('-',' ').replace(':',' ').replace('.',' ')
    
    dt_list = list(map(int, dt_str.split()))

    return dt_list


def centroid_params_from_string(centroid_string, dt_list):
    centroid_list = list(map(lev, centroid_string.split()[1:]))

    try:
        eq_datetime = dt.datetime(*dt_list)
        centroid_timedelta = dt.timedelta(seconds = centroid_list[0])
        centroid_time = eq_datetime + centroid_timedelta
        centroid_timestamp = centroid_time.isoformat(' ')
    except:
        # no failures tolerated
        #centroid_time = centroid_list[0]
        #centroid_timestamp = str(centroid_time)
        pass

    d = {'centroid_datetime'  : centroid_timestamp,
         'centroid_date' : centroid_timestamp.split()[0],
         'centroid_time' : centroid_timestamp.split()[1],
         'centroid_time_err'  : centroid_list[1],
         'centroid_latitude'  : centroid_list[2],
         'centroid_lat_err'   : centroid_list[3],
         'centroid_longitude' : centroid_list[4],
         'centroid_lon_err'   : centroid_list[5],
         'centroid_depth'     : centroid_list[6],
         'centroid_depth_err' : centroid_list[7]}

    return d


def parse_centroid_string(eq_dict):

    dt_list = datetime_string_to_list(eq_dict)

    centroid_string = eq_dict.pop('centroid_params')

    centroid_params = centroid_params_from_string(centroid_string, dt_list)

    eq_dict.update(centroid_params)
    return


def data_used_from_string(data_used_string):

    data_used_string = data_used_string.replace('B:','')
    data_used_string = data_used_string.replace('M:','')
    data_used_string = data_used_string.replace('S:','')

    du_list = data_used_string.split()

    d = {'body_wave_stations'           :   int(du_list[0]),
         'body_wave_components'         :   int(du_list[1]),
         'body_wave_shortest_period'    : float(du_list[2]),
         'surface_wave_stations'        :   int(du_list[3]),
         'surface_wave_components'      :   int(du_list[4]),
         'surface_wave_shortest_period' : float(du_list[5]),
         'mantle_wave_stations'         :   int(du_list[6]),
         'mantle_wave_components'       :   int(du_list[7]),
         'mantle_wave_shortest_period'  : float(du_list[8])}
    return d


def parse_data_used_string(eq_dict):
    try:
        du_string = eq_dict.pop('data_used')
    except KeyError:
        return

    du_d = data_used_from_string(du_string)
    eq_dict.update(du_d)
    return


def moment_rate_function_from_string(mr_string):
    mr_list = mr_string.split()

    d = {'moment_rate_half_duration' : float(mr_list[1])}

    if mr_list[0] == 'TRIHD:':
        d['moment_rate_function'] = 'triangle'
    elif mr_list[0] == 'BOXHD':
        d['moment_rate_function'] = 'boxcar'
    else:
        d['moment_rate_function'] = 'NA'

    return d

def parse_moment_rate_function_string(eq_dict):
    try:
        mr_string = eq_dict.pop('moment_rate_function_string')
    except KeyError:
        return

    mr_d = moment_rate_function_from_string(mr_string)
    eq_dict.update(mr_d)
    return


def format_data(eq_dict):

    parse_magnitude_string(eq_dict)
    parse_reference_locations_strings(eq_dict)
    parse_moment_tensor_params_string(eq_dict)
    parse_moment_tensor_axes_string(eq_dict)
    parse_fault_params_string(eq_dict)
    parse_scalar_moment_string(eq_dict)
    add_Mw(eq_dict)
    format_ref_date_string(eq_dict)
    get_ref_datetime(eq_dict)
    parse_centroid_string(eq_dict)
    parse_data_used_string(eq_dict) #not yet implemented
    parse_moment_rate_function_string(eq_dict) #not yet implemented
    return



'''
Functions for working on full NDK strings or files
'''

def parse_ndk_line_list(ndk_line_list, start_line_no=0, strip_data=True,
                        format_dict=True):

    ndk_1 = parse_ndk_line_1(ndk_line_list[start_line_no])
    ndk_2 = parse_ndk_line_2(ndk_line_list[start_line_no + 1])
    ndk_3 = parse_ndk_line_3(ndk_line_list[start_line_no + 2])
    ndk_4 = parse_ndk_line_4(ndk_line_list[start_line_no + 3])
    ndk_5 = parse_ndk_line_5(ndk_line_list[start_line_no + 4])

    ndk_dict = merge_dicts(ndk_1, ndk_2, ndk_3, ndk_4, ndk_5)

    if strip_data == True:

        ndk_dict = strip_dict(ndk_dict)

    if format_dict == True:
        format_data(ndk_dict)

    return ndk_dict


def make_ndk_line_list(ndk_string):
    '''
    Converts a string w/ linebreaks to a list of non-empty strings,
    one item for each line.
    '''
    ndk_list = [line for line in ndk_string.split('\n') if line.strip() != '']

    return ndk_list


def read_ndk_file(ndk_filepath):
    with open(ndk_filepath, 'r') as f:
        ndk_string = f.read()

    return ndk_string


def parse_ndk_string(ndk_string, strip_data=True):

    ndk_line_list = make_ndk_line_list(ndk_string)

    ndk_lines = len(ndk_line_list)
    n_events = ndk_lines // 5

    eq_list = []

    for event_no in range(n_events):

        try:
            line_start = event_no * 5

            event_dict = parse_ndk_line_list(ndk_line_list, 
                                             start_line_no=line_start,
                                             strip_data=strip_data)
            eq_list.append(event_dict)

        except SyntaxError as SE:
            #logging.warning(SE)
            event_name_line = event_no * 5 + 1
            event_name = ndk_line_list[event_name_line].split()[0]

            logging.warning(
                "EQ {} Couldn't be processed; check file lines {}-{}"
                .format(event_name, event_name_line-1, event_name_line+4))

    if n_events == 1:
        return eq_list[0]
    else:
        return eq_list


def parse_ndk_file(filepath):
    ndk_file_string = read_ndk_file(filepath)

    return parse_ndk_string(ndk_file_string)



'''
util functions
'''
def strip_dict(d):
    '''
    Takes a dictionary d, and returns a new dictionary with all of
    the values stripped if they are strings, and left alone otherwise.
    '''
    return {key: d[key].strip() if type(d[key])==str else d[key] for key in d} 


def merge_dicts(*dicts):
    '''
    Merges a list of dictionaries; nested dictionaries should stay nested.
    '''
    d_merge = {}

    for d in dicts:
        d_merge.update(d)

    return d_merge


import ast.literal_eval as lev


''' NDK string parsing functions'''

def parse_ndk_line_1(l1_string):
    l1 = {}
    l1['hyp_ref_catalog'] = l1_string[0:4]
    l1['ref_date'] = l1_string[5:15]
    l1['ref_time'] = l1_string[16:26]
    l1['depth'] = l1_string[42:47]
    l1['magnitudes'] = l1_string[48:55]       # needs further parsing
    l1['geog_location'] = l1_string[56:80]

    return l1
    

def parse_ndk_line_2(l2_string):
    l2 = {}
    l2['cmt_event_name'] = l2_string[0:16]
    l2['data_used'] = l2_string[17:61]         # need to parse further
    l2['cmt_type'] = l2_string[62:68]          # can be exanded
    l2['moment_rate_function'] = l2_string[69:80] # more parsing too

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
    l5['scalar_moment'] = l5_string[49:56]
    l5['fault_params'] = l5_string[57:81]

    return l5


def parse_ndk_line_list(ndk_line_list, start_line_no=0, strip_data=True):

    ndk_1 = parse_ndk_line_1(ndk_line_list[start_line_no])
    ndk_2 = parse_ndk_line_2(ndk_line_list[start_line_no + 1])
    ndk_3 = parse_ndk_line_3(ndk_line_list[start_line_no + 2])
    ndk_4 = parse_ndk_line_4(ndk_line_list[start_line_no + 3])
    ndk_5 = parse_ndk_line_5(ndk_line_list[start_line_no + 4])

    ndk_dict = merge_dicts(ndk_1, ndk_2, ndk_3, ndk_4, ndk_5)

    if strip_data == True:

        ndk_dict = {key: ndk_dict[key].strip() for key in ndk_dict}

    return ndk_dict


def make_ndk_line_list(ndk_file_or_string):
    # just a string for now
    ndk_string = ndk_file_or_string

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

        line_start = event_no * 5

        event_dict = parse_ndk_line_list(ndk_line_list, 
                                         start_line_no=line_start,
                                         strip_data=strip_data)

        eq_list.append(event_dict)

    return eq_list


''' util functions'''
def merge_dicts(*dicts):
    d_merge = {}

    for d in dicts:
        d_merge.update(d)

    return d_merge


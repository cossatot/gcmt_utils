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


from urllib.request import urlopen
import datetime
#import attrs

import pandas as pd
import numpy as np

from .ndk_parser import parse_ndk_string
from .sql_utils import column_key_correlation_d

col_key_d_rev = {val: key for key, val in column_key_correlation_d.items()}


'''
functions for getting ndk files from globalcmt.org, and processing them
'''
def get_bb_url(eq_d, bb_dir_path):

    cmt_name = eq_d['cmt_event_name']

    bb_url = bb_base_url.format(cmt_name)
    
    eq_d.update({'focal_mech': bb_url})


jan76_dec2013_ndk_url = ('http://www.ldeo.columbia.edu/~gcmt/projects/CMT/'
                         + 'catalog/jan76_dec13.ndk')

base_monthly_url = ('http://www.ldeo.columbia.edu/~gcmt/projects/CMT/'
                    + 'catalog/NEW_MONTHLY/{}/{}.ndk')

quick_cmt_url = ('http://www.ldeo.columbia.edu/~gcmt/projects/CMT/catalog/'
                 + 'NEW_QUICK/qcmt.ndk')

#years = ['2014', '2015', '2016', '2017', '2018']
months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
          'jul', 'aug', 'sep', 'oct', 'nov', 'dec']


def format_monthly_url(year, mo, base_monthly_url=base_monthly_url):
    yr_mo_string = mo + year[2:]

    return base_monthly_url.format(year, yr_mo_string)


def get_year_list(start_year=2014):
    current_year = datetime.datetime.now().year

    year_list = [start_year]

    n_years = current_year - start_year

    for i in range(n_years):
        i += 1
        year_list.append(start_year + i)

    return [str(year) for year in year_list]


def get_monthly_url_list():
    years = get_year_list()

    return [format_monthly_url(yr, mo) for mo in months for yr in years]


def download_base_ndk(url=jan76_dec2013_ndk_url):
    return urlopen(url).read().decode('utf-8')


def download_monthly_ndks():
    monthly_url_list = get_monthly_url_list()

    mo_data_list = []

    for mo_url in monthly_url_list:
        try:
            mo_data = urlopen(mo_url).read().decode('utf-8')
            mo_data_list.append(mo_data)

        except: #HTTPError:
            # should log this
            pass

    return mo_data_list

def process_catalog_ndks():
    
    # get ndks
    base_ndks = download_base_ndk()
    monthly_ndks = download_monthly_ndks()

    # process them, yielding a list of `eq_dicts`
    eq_dict_list = parse_ndk_string(base_ndks)

    for mo_string in monthly_ndks:
        eq_dict_list += parse_ndk_string(mo_string)

    eq_list = [GCMT_event.from_eq_dict(eqd) for eqd in eq_dict_list]

    return eq_list


# functions for calculating zoom thresholds on viewer
def calc_min_zoom(vals, zoom_threshold):
    '''
    Takes a list of Mw values for earthquakes in a region/bin,
    makes a 'rank' (i.e. the position in a decreasing-sorted Mw list),
    and yields a min zoom based on this for each earthquake
    '''
    ranks = len(vals) - vals.argsort().argsort()
    
    return zoom_threshold[np.searchsorted(zoom_threshold[:,0], ranks),1]


def min_dens(zoom, scale=1.5):
    if zoom < 5:
        return 1
    else:
        return int(2 **((zoom - 5) * scale))


def add_min_zoom(eq_list, bin_size_degrees=1., min_zoom=1, max_zoom=12):
    '''
    Calculates the minimum zoom threshold for each earthquake to appear in
    the GMCT Viewer based on the regional earthquake density and the 
    relative size of each earthquake compared to its neighbors
    '''

    eq_df = pd.concat([eq.to_dataframe(columns=['Longitude','Latitude','Mw'])
                       for eq in eq_list])

    # edges for longitude and latitude bins
    lon_edges = np.linspace(-180,180, (360 / bin_size_degrees) +1)
    lat_edges = np.linspace(-90,90, (180 / bin_size_degrees) +1)

    # indices for which lon and lat bin each earthquake falls into
    lon_idxs = np.searchsorted(lon_edges, eq_df.Longitude) -1
    lat_idxs = np.searchsorted(lat_edges, eq_df.Latitude) -1

    # joint index
    eq_df['ll_tuple'] = tuple(zip(lon_idxs, lat_idxs))

    # array of zoom thresholds: right col is number of events at zoom level,
    # left col is zoom level
    zoom_thresholds = np.array([[min_dens(e), e] for e in np.arange(min_zoom,
                                                                    max_zoom)])

    # group earthquakes by spatial index, rank, add min zoom values to each
    eq_df['min_zoom'] = eq_df.groupby('ll_tuple')['Mw'].transform(calc_min_zoom,
                                                              zoom_thresholds)
    eq_mzs = eq_df.min_zoom.values

    _ = [eq.set_minZoom(eq_mzs[i]) for i, eq in enumerate(eq_list)]

    return eq_list

    
'''
other stuff
'''

_title_string = "Mw: {:0.1f} Date: {} Depth: {} km"

_default_dataframe_cols = list(column_key_correlation_d.keys())
_default_dataframe_cols.remove('Focal_mech')

class GCMT_event(object):
    
    def __init__(self,
                 Ms: float = None,
                 Mw: float = None,
                 Body_wave_components : int = None,
                 Body_wave_period : float = None,
                 Body_wave_stations: int = None,
                 Date: str = None,
                 Datetime: str = None,
                 Depth:float = None,
                 Depth_err: float = None,
                 Lat_err: float = None,
                 Latitude: float = None,
                 Lon_err:  float = None,
                 Longitude: float = None,
                 Time: str = None, 
                 Time_err: str = None,
                 CMT_Version: str = None,
                 Event: str = None,
                 CMT_Timestamp: str = None,
                 CMT_Type: str = None,
                 Depth_inversion_type: str = None,
                 Dip_1: float = None,
                 Dip_2: float = None,
                 Geog_location: str = None,
                 Hypocenter_reference_catalog: str = None,
                 Mantle_wave_components: int = None,
                 Mantle_wave_period: float = None,
                 Mantle_wave_stations: int = None,
                 Mb: float = None,
                 Moment_rate_function: str = None,
                 Moment_rate_half_duration: float = None,
                 Mpp: float = None,
                 Mpp_err: float = None,
                 Mrp: float = None,
                 Mrp_err: float = None,
                 Mrr: float = None,
                 Mrr_err: float = None,
                 Mrt: float = None,
                 Mrt_err: float = None,
                 Moment_exp: int = None,
                 Mtp: float = None,
                 Mtp_err: float = None,
                 Mtt: float = None,
                 Mtt_err: float = None,
                 N: float = None,
                 N_azimuth: float = None,
                 N_plunge: float = None,
                 P: float = None,
                 P_azimuth: float = None,
                 P_plunge: float = None,
                 Rake_1: float = None,
                 Rake_2: float = None,
                 Reference_date: str = None, #datetime
                 Reference_datetime: str = None, #datetime
                 Reference_depth: float = None,
                 Reference_latitude: float = None,
                 Reference_longitude: float = None,
                 Reference_time: str = None, #datetime
                 Scalar_moment: float = None,
                 Strike_1: float = None,
                 Strike_2: float = None,
                 Surface_wave_components: int = None,
                 Surface_wave_period: float = None,
                 Surface_wave_stations: int = None,
                 T: float = None,
                 T_azimuth: float = None,
                 T_plunge: float = None,
                 Focal_mech: str = None):
                
        # attributes from init (dict) arguments
        self.Ms = Ms
        self.Mw = Mw
        self.Body_wave_components = Body_wave_components
        self.Body_wave_period = Body_wave_period
        self.Body_wave_stations= Body_wave_stations
        self.Date = Date
        self.Datetime= Datetime
        self.Depth= Depth
        self.Depth_err = Depth_err
        self.Lat_err = Lat_err
        self.Latitude = Latitude
        self.Lon_err = Lon_err
        self.Longitude = Longitude
        self.Time = Time
        self.Time_err = Time_err
        self.CMT_Version = CMT_Version
        self.Event = Event
        self.CMT_Timestamp = CMT_Timestamp
        self.CMT_Type = CMT_Type
        self.Depth_inversion_type = Depth_inversion_type
        self.Dip_1 = Dip_1
        self.Dip_2 = Dip_2
        self.Geog_location = Geog_location
        self.Hypocenter_reference_catalog = Hypocenter_reference_catalog
        self.Mantle_wave_components = Mantle_wave_components
        self.Mantle_wave_period = Mantle_wave_period
        self.Mantle_wave_stations = Mantle_wave_stations
        self.Mb = Mb
        self.Moment_rate_function = Moment_rate_function
        self.Moment_rate_half_duration = Moment_rate_half_duration
        self.Mpp = Mpp
        self.Mpp_err = Mpp_err
        self.Mrp = Mrp
        self.Mrp_err = Mrp_err
        self.Mrr = Mrr
        self.Mrr_err = Mrr_err
        self.Mrt = Mrt
        self.Mrt_err = Mrt_err
        self.Moment_exp = Moment_exp
        self.Mtp = Mtp
        self.Mtp_err = Mtp_err
        self.Mtt = Mtt
        self.Mtt_err = Mtt_err
        self.N = N
        self.N_azimuth = N_azimuth
        self.N_plunge = N_plunge
        self.P = P
        self.P_azimuth = P_azimuth
        self.P_plunge = P_plunge
        self.Rake_1 = Rake_1
        self.Rake_2 = Rake_2
        self.Reference_date = Reference_date
        self.Reference_datetime = Reference_datetime
        self.Reference_depth = Reference_depth
        self.Reference_latitude = Reference_latitude
        self.Reference_longitude = Reference_longitude
        self.Reference_time = Reference_time
        self.Scalar_moment = Scalar_moment
        self.Strike_1 = Strike_1
        self.Strike_2 = Strike_2
        self.Surface_wave_components = Surface_wave_components
        self.Surface_wave_period = Surface_wave_period
        self.Surface_wave_stations = Surface_wave_stations
        self.T = T
        self.T_azimuth = T_azimuth
        self.T_plunge = T_plunge

        # additional attributes
        self.minZoom = None
        self.icon = self.get_icon_dict()
        self.title = _title_string.format(self.Mw, self.Date, self.Depth)

    # methods
    def get_icon_size(self):
        return int(0.7 * self.Mw**2.1)

    def get_icon_dict(self,
                      bb_base_url = ("http://earth-analysis.com/gcmt_viewer/"
                                      + "data/beachballs/png_reduced/{}.png")):
        icon_size = self.get_icon_size()
        icon_dict = {'icon': {'iconSize': [icon_size, icon_size],
                              'iconUrl': bb_base_url.format(self.Event)
                              }
                     }
        return icon_dict

    def set_minZoom(self, min_zoom):
        self.minZoom = min_zoom

    def to_feature_dict(self, properties=['icon', 'title', 'Mw', 'Depth',
                                          'Date', 'Event', 'minZoom']):
        
            
        fd =  {'geometry': {
                           'type': 'Point',
                           'coordinates' : [self.Longitude, 
                                            self.Latitude]
                            },
               'type': 'Feature'
               }

        fd['properties'] = {p: self.__dict__[p] for p in properties}

        return fd

    def to_dataframe(self, columns=_default_dataframe_cols, name=None):

        if name is None:
            name = self.Event

        return pd.DataFrame({col: self.__dict__[col] for col in columns},
                            index=[name])


    @classmethod
    def from_eq_dict(cls, eq_dict):
        
        # transmogrify eq_dict (from ndk parsing) to dict w/ correct
        # class initiation (and database column) keys
        kw_dict = {col_key_d_rev[k] : v for k, v in eq_dict.items()}

        return cls(**kw_dict)





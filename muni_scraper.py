# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 18:41:38 2016

@author: nikolauspschuetz
"""

import math
import random
import re
import time
import sqlite3
import datetime as dt
import numpy as np
import pandas as pd
from matplotlib import path as mplPath
from matplotlib import pyplot as plt
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from geopy.geocoders import Nominatim


class muniScraper(object):
    """ Opens a chrome driver with selenium and navigates to google maps; samples
    locations in San Francisco; and collects and returns: travel times between locations
    for multiple methods of transport (by car, bus, bike, and foot), for both
    outbound and return travel, plus timestamp and street addresses.
    
    -------------------------------------------------------------------------

    Note: Usage of the data collected through muniScraper is intended to show
    routes in SF where public transportation is at a particular disadvantage,
    relative to travel by car or bike.
    (E.g., pub_transit_factor = [transit time] / [driving time])
    This could be used...
    a) As a sorted dataframe and visualized from there...
    b) A user could select a location in SF, and then the travel times to all
        other locations can be displayed. The implementation could look as follows:
        1. Select a subset of data where the depart location is within some
            radius of the selected location.
            Note also that, because latitude and longitude are not
            on the same scale (especially at SF's latitude), in order to draw
            a radius of N meters around the depart location on the surface
            of the earth, the analyst must draw an ellipse in the data!
        2. Use the arrive location for the travel times
        3. Pick a granularity for datapoints within SF: the subset of data selected
            in step 1 will be incomplete. Therefore, interpolate the missing data.
            It is the analyst's responsibility to make sure there is enough data
            in the subset (i.e., the radius around the depart location is big enough)
            that the interpolated travel times are robust.
        4. The data are now ready to be visualized
    
    -------------------------------------------------------------------------
    
    Note: google maps has an API, through which this would be A LOT EASIER.
    HOWEVER: this has a cap on the number of free requests per day: 1,500.
    Each arrive, depart trip would require four requests: one for each travel
    mode (car, transit, bike, foot). Thherefore this method is too slow to ever
    collect a big enough sample for analysis.
    
    for googlemaps package see: https://pypi.python.org/pypi/googlemaps/
    for keys & pricing see: https://developers.google.com/api-client-library/python/
    
    -------------------------------------------------------------------------

    Usage example for muniScraper.run():
    
    # the user may have to pip install [packages used in this class]
    
    import sys
    sys.path.append('/path/to/muni_scraper/')
    
    import muni_scraper

    import numpy as np
    
    kwargs = {'chrome_options_arg':'--incognito',
              'base_url':'https://www.google.com/maps/dir/',
              'driver_path':[path/to/chromedriver]}
    
    ms = muni_scraper.muniScraper(**kwargs)
    
    # open a db path with Database() class
    db_path = '/path/to/database.db'
    gdb = muni_scraper.Database(db_path, buffer_size=10)
    
    # run for some n of N samples (collects <= two records per n)
    for n in range(int(1e5)):
        # second for is because ms.run() returns a df with two rows
        for dat in np.array(ms.run()):
            gdb.record(dat)
        time.sleep(1)    
    """
    
    # For finding elements with selenium
    dirsb = "directions-searchbox-"
    tsbi = "tactile-searchbox-input"
    wdi_reverse = ".widget-directions-icon.reverse"
    wpsdtd = "widget-pane-section-directions-trip-duration"
    dtme = "directions-travel-mode-expander"
    wpsdept = "widget-pane-section-directions-error-primary-text"

    # column names    
    columns = ("arrive_add", "arrive_lat", "arrive_lon", "bicycle", "depart_add",
               "depart_lat", "depart_lon", "driving", "transit", "walk", "timestamp")

    # dictionary of directions method:selenium element key
    dir_dict = {"driving":".directions-travel-mode-icon.directions-drive-icon",
                "transit":".directions-travel-mode-icon.directions-transit-icon",
                "walk":".directions-travel-mode-icon.directions-walk-icon",
                "bicycle":".directions-travel-mode-icon.directions-bicycle-icon"}

    # for each run(), use this df structure and print these cols
    timesdf = pd.DataFrame(columns=columns, index=(0,1))        
    print_cols = ["timestamp", "depart_lat", "depart_lon", "arrive_lat",
                  "arrive_lon", "driving", "transit", "bicycle", "walk"]
    
    # lat-lon coordinates for the perimeter of SF
    sf_perim = [(37.708266, -122.393657), (37.708440, -122.485511), (37.724103, -122.485019),
                (37.726958, -122.483816), (37.729338, -122.485621), (37.730333, -122.489504),
                (37.728905, -122.491802), (37.729554, -122.493661), (37.729121, -122.496561),
                (37.731501, -122.498748), (37.725358, -122.503452), (37.727045, -122.506133),
                (37.732658, -122.507427), (37.735427, -122.506825), (37.775045, -122.511305),
                (37.778698, -122.513720), (37.779853, -122.509438), (37.780845, -122.509633),
                (37.781737, -122.493314), (37.787528, -122.493787), (37.787908, -122.491871),
                (37.787112, -122.491174), (37.788406, -122.489796), (37.788844, -122.489989),
                (37.790350, -122.485654), (37.810905, -122.477056), (37.809361, -122.476045),
                (37.808615, -122.471663), (37.803555, -122.459563), (37.804439, -122.453015),
                (37.805638, -122.453891), (37.806730, -122.447791), (37.805212, -122.447251),
                (37.805851, -122.442263), (37.806650, -122.442499), (37.807502, -122.435893),
                (37.806277, -122.435657), (37.804999, -122.433769), (37.806064, -122.425545),
                (37.807901, -122.421703), (37.808061, -122.417793), (37.808301, -122.415838),
                (37.809073, -122.415939), (37.808860, -122.412669), (37.807768, -122.407479),
                (37.806384, -122.404715), (37.803774, -122.401547), (37.792636, -122.390954),
                (37.791064, -122.389047), (37.789640, -122.388360), (37.787995, -122.387738),
                (37.778371, -122.387480), (37.777149, -122.390399), (37.776547, -122.389927),
                (37.776615, -122.387459), (37.771519, -122.386676), (37.768839, -122.385056),
                (37.765330, -122.386579), (37.763624, -122.387109), (37.763124, -122.386537),
                (37.763081, -122.385244), (37.762142, -122.385285), (37.761762, -122.383279),
                (37.759570, -122.381891), (37.759547, -122.381410), (37.758147, -122.381303),
                (37.757705, -122.381607), (37.755286, -122.381178), (37.755058, -122.384233),
                (37.754531, -122.384091), (37.754464, -122.383106), (37.754479, -122.383072),
                (37.753239, -122.383082), (37.753120, -122.384104), (37.748432, -122.382347),
                (37.748126, -122.386309), (37.747807, -122.390386), (37.748806, -122.392968),
                (37.747466, -122.393263), (37.746633, -122.390767), (37.746935, -122.375659),
                (37.740055, -122.367982), (37.739688, -122.374093), (37.732913, -122.375795),
                (37.732139, -122.374232), (37.734052, -122.372238), (37.731735, -122.369059),
                (37.731634, -122.365114), (37.729890, -122.362227), (37.728079, -122.362141),
                (37.728514, -122.357474), (37.726166, -122.357899), (37.725731, -122.365537),
                (37.723636, -122.362980), (37.722097, -122.364197), (37.719901, -122.363036),
                (37.716778, -122.364195), (37.724967, -122.377806), (37.722607, -122.381104),
                (37.724809, -122.387145), (37.721685, -122.383194), (37.720367, -122.383779),
                (37.716056, -122.376343), (37.709382, -122.381688), (37.710458, -122.390474),
                (37.708142, -122.393783)]
        
    def __init__(self, driver_path, base_url, chrome_options_arg, wait_time=1.5e7):
        
        self.driver_path = driver_path
        self.wait_time = wait_time

        chrome_options = webdriver.ChromeOptions()
        if chrome_options_arg:
            chrome_options.add_argument(chrome_options_arg)
        
        self.chrome_options = chrome_options        

        self.driver = webdriver.Chrome(executable_path=self.driver_path,
                                       chrome_options=self.chrome_options)
        
        # Note: selenium can be run headless, with phantomJS
        # however, this does not seem to work for google maps
        # self.driver = webdriver.PhantomJS(executable_path=self.driver_path, service_log_path=os.path.devnull)
        # self.driver.set_window_size(1366,768)
        
        self.base_url = base_url
        self.driver.get(self.base_url)

        self.geolocator = Nominatim(timeout=None)

        # the perim of SF in matplotlib
        self.sfPath = mplPath.Path(np.array(self.sf_perim))
        
        # initialize some other views of the sf perim for easy use later
        self.lats = [x for x, _ in self.sf_perim]
        self.lons = [x for _, x in self.sf_perim]
        self.ylim = self.ymin, self.ymax = np.array(self.lats).min(), np.array(self.lats).max()
        self.xlim = self.xmin, self.xmax = np.array(self.lons).min(), np.array(self.lons).max()
        
    def get_address(self, latlon):
        """ Uses geolocator to get a street address
        'latlon' = (latitude, longigude) tuple """
        loc = self.geolocator.reverse(latlon)
        return loc.address

    def get_cn_text(self, cn_name):
        """ Finds elements by class name and returns all non-empty text """
        return [elmt.text for elmt in self.driver.find_elements_by_class_name(self.wpsdtd) if elmt.text != ""]

    def get_times(self):
        """ Semi-redundant function for handling slow loading of page.
        It allows for get_traveltimes to run until it is populated with the text
        from at least one element, or until self.wait_time is exceded. """
        gtt = None
        wttm = 0
        t0 = dt.datetime.now()
        while not gtt and wttm <= self.wait_time:
            try:
                gtt = self.get_traveltimes()
                wttm = (dt.datetime.now() - t0).microseconds
            except:
                next
        return gtt

    def get_traveltimes(self):
        """ Gets travel times for each method of transportation, from a->b """
        onewaytimes = dict()
        for key in self.dir_dict.keys():
            # hover over the travel mode expander
            dtme = self.driver.find_element_by_class_name(self.dtme)
            # this ensures all travel mode elements are clickable
            ActionChains(self.driver).move_to_element(dtme).perform()
            self.driver.find_element_by_css_selector(self.dir_dict[key]).click()
            # wait for the data to load
            time.sleep(1)
            #sometimes there are no directions available
            if not [x.text for x in self.driver.find_elements_by_class_name(self.wpsdept) if x.text]:
                next
            wait_counter = 0
            while wait_counter < 10 and len(self.get_cn_text(self.wpsdtd)) < 1:
                time.sleep(1 / math.e)
                wait_counter += 1
            text = self.get_cn_text(self.wpsdtd)
            if text:
                onewaytimes[key] = self.parse_times(text[0])
            else:
                onewaytimes[key] = None
        
        return onewaytimes

    def in_sf(self, tup):
        """ Tests if the supplied tuple of geocordinates is inside the perimeter of SF.
        'tup' = tuple of geocoordinates. """
        return self.sfPath.contains_point(tup) == 1

    def initialize_directions(self, depart, arrive):
        """ Requests initial directions from google maps (transportation method agnostic).
        'depart' = 'depart' = tuple of latitude, longitutde coordinates """
        sb_dict = dict()
        for location, i in zip((depart, arrive), (0, 1)):
            i = str(i)
            sb_dict["dsb" + i] = self.driver.find_element_by_id(self.dirsb + i)
            sb_dict["tsi" + i] = sb_dict["dsb" + i].find_element_by_class_name(self.tsbi)
            sb_dict["tsi" + i].clear()
            sb_dict["tsi" + i].send_keys(str(location)[1:-1])
            sb_dict["tsi" + i].send_keys(Keys.RETURN)

    def parse_times(self, timestr):
        """ Turns the travel times from string to integer.
        'timestr' = single travel time, as a string
        Example:
        parse_times('47m') returns 47
        parse_times('1h 12m') returns 72 """
      
        hh = mm = 0
        if "h" in timestr:
            hh = int(re.search("[1-9] h", timestr).group().replace(" h",""))
            hh *= 60
        if "m" in timestr:
            mm = int(re.search("[0-9]{0,2} m", timestr).group().replace(" m",""))
        return hh + mm

    def plot_sf(self, title="SF Perimeter"):
        """ Plot the permieter of SF """
        plt.figure(figsize=(8,8))
        plt.plot(self.lons, self.lats)
        plt.ylim(self.ylim)
        plt.xlim(self.xlim)
        plt.title(title)
        plt.show()

    def restart_driver(self):
        self.driver.close()
        self.driver = webdriver.Chrome(executable_path=self.driver_path,
                                       chrome_options=self.chrome_options)
        self.driver.get(url=self.base_url)
        time.sleep(math.e)
    
    def reverse_direction(self):
        """ Finds the element to reverse directions, and simualates a click on the
        'reverse directions' button. """
        self.driver.find_element_by_css_selector(self.wdi_reverse).click()
        
    def run(self):
        """ Uses two randomly sampled latitude, longitude locations in SF,
        (depart & arrive), gets their street addresses (for later use in analysis),
        collects travel times to and from the locations, and returns a dataframe
        with two rows (typically; in the event of nulls, that row is excluded),
        one for outbound, one for inbound. For each trip, the tiemstamp is recorded. """
        timesdf = self.timesdf
        arrive, depart = self.sample_locations()
        for dirstr, dirtpl in zip(("arrive", "depart"), (arrive, depart)):
            timesdf.loc[0, [dirstr + "_lat", dirstr + "_lon"]] = dirtpl
            timesdf.loc[0, dirstr + "_add"] = self.get_address(dirtpl)
        self.initialize_directions(arrive, depart)
        gt = self.get_times()
        timesdf.loc[0, "timestamp"] = pd.to_datetime(dt.datetime.now())
        if gt:
            for key, val in gt.items():
                timesdf.loc[0, key] = val
        # reverse
        arrive, depart = depart, arrive
        for dirstr, dirtpl in zip(("arrive", "depart"), (arrive, depart)):
            timesdf.loc[1, [dirstr + "_lat", dirstr + "_lon"]] = dirtpl
        timesdf.loc[1, "arrive_add"], timesdf.loc[1, "depart_add"] = timesdf.loc[0, "depart_add"], timesdf.loc[0, "arrive_add"]
        self.reverse_direction()
        gt = self.get_times()
        timesdf.loc[1, "timestamp"] = pd.to_datetime(dt.datetime.now())
        if gt:
            for key, val in gt.items():
                timesdf.loc[1, key] = val

        timesdf = timesdf.ix[timesdf.isnull().sum(1) == 0]
        # take only full columns, change datatype, return
        for colname in ["arrive_lat", "arrive_lon", "depart_lat", "depart_lon"]:
            timesdf[colname] = timesdf[colname].astype(np.float64)
        for colname in ["bicycle", "driving", "transit", "walk"]:
            timesdf[colname] = timesdf[colname].astype(np.int64)
        timesdf["timestamp"] = timesdf["timestamp"].astype(str)
        print(timesdf.loc[:, self.print_cols])
        return timesdf

    def sample_locations(self):
        """ Returns lat, lon for two random locations in SF """
        return [self.sample_sf() for _ in range(2)]
        
    def sample_sf(self):
        """ Returns a randomly sampled lat, lon location in SF """
        insf = 0
        while not insf == 1:
            sfpoint = tuple(map(lambda x: random.uniform(*x), [self.ylim, self.xlim]))
            insf = self.sfPath.contains_point(sfpoint)
        return sfpoint


class Database(object):
    """ Take the information from Google Maps and stuff it into a database """

    fields = ("arrive_add", "arrive_lat", "arrive_lon", "bicycle", "depart_add",
              "depart_lat", "depart_lon", "driving", "transit", "walk", "timestamp") 

    dtypes = ("text", "real", "real", "integer", "text", "real",
              "real", "integer", "integer", "integer", "text")
    
    def __init__(self, path, buffer_size=100):
        # turn on the database. if the path doesnt exist, that means the table
        # needs to be created

        # create commands
        tmp_f = ", ".join(["%s %s"%(f, dt) for f, dt in zip(self.fields, self.dtypes)])
        create_cmd = "create table google_responses (%s)" % tmp_f
        self.insert_cmd = "insert into google_responses values (%s)"%(", ".join(["?"]*len(self.fields)))

        from os.path import isfile
        create = not isfile(path)

        self.conn = sqlite3.connect(path)
        self.curs = self.conn.cursor()

        # create table if necessary
        if create:
            self.curs.execute(create_cmd)

        self.buffer_size = buffer_size
        self.data_buffer = []

    def record(self, data):

        # check the data. should be: datetime, float, float, float, float, list, list
        for idx, dtype in enumerate((object, float, float, int, object, float, float, int, int, int, object)):
            if not isinstance(data[idx], dtype):
                print("failed!")
                return None

        # append data to data_buffer
        self.data_buffer.append(tuple(data))

        # if the data_buffer has reached a certain length, dump the data into
        # the database and clear the buffer. sqlite3 is optimized for transactions
        # so it makes sense to wait a while before making a write in order to
        # amortize the cost of disk access
        if len(self.data_buffer) >= self.buffer_size:
            self.curs.executemany(self.insert_cmd, self.data_buffer)
            self.conn.commit()
            self.data_buffer = []

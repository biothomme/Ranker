# Here one can find the functionalities for the
# overall wrapper that makes mutliple database
# requests according to user choice and defined
# locations

import os
import shapely
import datetime

import database_classes
from utils import set_directory
from utils import set_locations
# core class
class GetMultiDBData:
    # the most necessary parameters are locations (shapely.Point
    # or list of it) and a root directory (str) to store the data in.
    locations = None
    root = None
    temporary = False  # defines if directory is temp, to delete it later.
    
    # this list should comprise all database specific functions that
    # should be mined.
    dataminers = []
    
    # less important are date of each sample (datetime.date) and 
    # a the tile size in metres (float)
    date = None
    tile_size = 50  # tile dimension (tile_size X tile_size) in metres (?!) 
    # TODO #

    # constructor
    def __init__(self, longitudes, latitudes, directory=None):
        self.locations = set_locations(longitudes, latitudes)
        self.root = set_directory(directory)
        if directory is None:
            self.temporary = True

    # add new database miners to list.
    def add_dataminers(self, new_dataminers):
        '''
        Add object(s) of subclasses of database_classes.SpatialData to current list.
        Duplicates are removed.
        '''
        is_spatialdata = lambda x: isinstance(x, database_classes.SpatialData)
        if type(new_dataminers) == list:
            self.dataminers.extend(filter(is_spatialdata, new_dataminers))
        elif is_spatialdata(new_dataminers):
            self.dataminers.append(new_dataminers)
        else:
            print(f"It is impossible to add {new_dataminers}, as it it no function"
                    " or list of it.")
        self.dataminers = list(set(self.dataminers))
        
        return

    # run the datamining an index location
    def run_index(self, index):
        assert type(index) == int, f"Index {index} needs to be an integer value."
        assert index < len(self.locations), (
                f"Index {index} was too large. Max index is len(self.locations)")
        assert index > 0, f"Index {index} needs to be positive."
        for dataminer in self.dataminers:
            dataminer(self.locations[index],
                    date=self.dates[index],
                    tile_size=self.tile_size,
                    directory=self.directory)  # run the mining on each database. 
        return




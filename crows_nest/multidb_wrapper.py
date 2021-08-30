# Here one can find the functionalities for the
# overall wrapper that makes mutliple database
# requests according to user choice and defined
# locations

import os
import shapely
from datetime import date

import database_classes
from utils import set_directory
from utils import set_locations
from utils import check_locations_and_dates

# core class
class GetMultiDBData:
    # the most necessary parameters are locations (shapely.Point
    # or list of it) and a root directory (str) to store the data in.
    root = None
    temporary = False  # defines if directory is temp, to delete it later.
    
    # this list should comprise all database specific functions that
    # should be mined.
    dataminers = []
    
    # also we use the date of each sample (datetime.date) and 
    # a the tile size in metres (float)

    # constructor
    def __init__(self, dataminers, directory=None):
        self.add_dataminers(dataminers)
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
            # we need to avoid instances that are no dataminers
            # and initialize a subdirectory within the central
            # multidb directory, if not explicitly wished differently.
            real_dataminers = filter(is_spatialdata, new_dataminers)
            for dataminer in real_dataminers:
                dataminer.set_db_directory(self.root)

            self.dataminers.extend(filter(is_spatialdata, new_dataminers))
        
        elif is_spatialdata(new_dataminers):
            self.dataminers.append(new_dataminers)
        
        else:
            print(f"It is impossible to add {new_dataminers}, as it is no function"
                    " or list of it.")
        # TODO: it could be problematic if someone adds
        #       two instances of the same dataminer class
        # self.dataminers = list(set(self.dataminers))
        
        return

    # run the datamining on provided locations and dates
    def run(self, locations, dates=None):
        '''
        Retrieve data from all requested databases for a given set of
        locations (and corresponding dates)
        '''
        # for each location we should have a respective date
        dates = check_locations_and_dates(locations, dates)
        for dataminer in self.dataminers:
            dataminer.run(locations, dates=dates)  # run the mining on each database. 
        return




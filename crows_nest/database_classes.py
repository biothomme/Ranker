# This file provides core utilities for the multiple datamining
# project on geolocations

from abc import ABCMeta, abstractmethod
from datetime import date
import os
import pickle
import rtree
import urllib

from utils import set_directory
from utils import download_to_path
from utils import check_locations_and_dates

# Core abstract class that provides important methods for all subclasses
class SpatialData(metaclass=ABCMeta):
    # authenticate, if necessary
    @abstractmethod
    def authenticate(self):
        pass

    # build the query
    @abstractmethod
    def build_query(self, location):
        pass  # return query
    
    # download the data
    @abstractmethod
    def get_data(self, build_query, file_name, location):
        pass  # return data_retrieved


    # produce query, request and store data
    def run(self, locations, dates=None):
        '''
        try to download data from defined database for given location.
        '''
        self.authenticate()
        self.size = len(locations)
        # for each location we should have a respective date
        dates = check_locations_and_dates(locations, dates)

        # the search should be run for each location
        for idx, (location, dt) in enumerate(zip(locations, dates)):
            file_name = self.make_file_name(idx, self.size)
            queries = self.build_query(location, dt)
            print(queries)
            data_files = self.get_data(queries, file_name, location, dt)
            
        return

    # set a directory for databaseminer class and store in self
    # this is important to allow flexibility in storing of
    # multiple dataminers
    def set_db_directory(self, directory):
        self.root = set_directory(directory)
        self.database_dir = os.path.join(
                self.root,
                self.database.lower().replace(" ", "_"))
        return

    # TODO make abstract method to check if data for location is defined
    def check_location(self, location):
        pass
    
    # allow to change tilesize
    def set_tile_size(self, tile_size):
        self.tile_size = tile_size
        return



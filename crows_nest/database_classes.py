# This file provides core utilities for the multiple datamining
# project on geolocations

from abc import ABCMeta, abstractmethod
import os
import pickle
import rtree
import urllib

from utils import set_directory
from utils import download_to_path

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
    def get_data(self, build_query):
        pass  # return data_retrieved


    # produce query, request and store data
    def run(self, location):
        '''
        try to download data from NAIP for given location.
        '''
        self.authenticate()
        # the NAIP database used here can be accessed efficiently by first downloading
        # a tile index, which is used in a second step to retrieve the necessary tiles.
        # to load the index, we use a subsequent function.
        self.location = location
        
        queries = self.build_query()
        print(queries)
        #try:
        data_files = self.get_data(queries)
        #except:
        #    print(f"It was not possible to fetch data for location {location}")
        #else:
        #    print(f"Data for location {location} was downloaded as "
        #            "{[df for df in data_files] if type(data_files) == list else datafiles}")
        return # TODO WAS NOT TESTED YET!!!!

    # store the data 
    def store_data(self, data_retrieved):
        pass

    # check if data for location is defined
    def check_location(self, location):
        pass




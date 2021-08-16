# This file provides core utilities for the multiple datamining
# project on geolocations

from abc import ABCMeta, abstractmethod

from utils import set_directory


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

    # store the data 
    def store_data(self, data_retrieved):
        pass

    # check if data for location is defined
    def check_location(self, location):
        pass


# class for queries of the NAIP database
class NAIPData(SpatialData):
    from multidb_wrapper import set_directory

    def __init__(self, tile_size=None, directory=None):
        self.database = "NAIP western europe Azure"
        self.root = set_directory(directory)

    def run(self, location):
        '''
        try to download data from NAIP for given location.
        '''
        self.authenticate()
        # the NAIP database used here can be accessed efficiently by first downloading
        # a tile index, which is used in a second step to retrieve the necessary tiles.
        # to load the index, we use a subsequent function.
        self.prepare()
        query = self.build_query(location)
        try:
            self.get_data(query)
        except:
            print(f"It was not possible to fetch data for location {location}")

    def authenticate(self):
        print("logged in")

    def build_query(self, location, date=None):
        '''
        Construct a query for NAIP database from coordinates and date
        '''
        return

    def get_data(self):
        print(self.database)

    def prepare(self):
        # TODO #
        if os.exists ...self.root()

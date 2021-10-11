# This file provides core utilities for the multiple datamining
# project on geolocations

from abc import ABCMeta
from abc import abstractmethod
from datetime import date
import json
import os
import pickle
import rtree
import urllib

from utils import set_directory
from utils import download_to_path
from utils import check_locations_and_dates
from utils import write_csv_row

from source_destination import LocalSourceDest
from source_destination import RemoteSourceDest

# Core abstract class that provides important methods for all subclasses
class SpatialData(metaclass=ABCMeta):
    '''
    Base class for all spatial dataminers.
    '''
    # statically load the sources for all databases
    with open("remote_sources.json", "r") as rs_file:
        REMOTE_SOURCE = json.load(rs_file)
    
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
            data_files = self.get_data(queries, file_name, location, dt)
            
        return

    # set a directory for databaseminer class and store in self
    # this is important to allow flexibility in storing of
    # multiple dataminers
    def set_db_directory(self, directory, multi_db_access=False):
        # we avoid overwriting a userdefined destination
        # by the multibd wrapper
        if multi_db_access and self.user_defined:
            if not self.silent:
                print(f"For the database `{self.database}` "
                     "a used defined destination but not the "
                      "multi_db directory will be used.")
            return
        
        # setting new direcories
        self.root = set_directory(directory)
        self.database_dir = os.path.join(
                self.root,
                self.DATABASE.lower().replace(" ", "_"))
        
        # after setting a new destination, the SourceDest objects need to be updated(if already set)
        if hasattr(self, "datasource"):
            if isinstance(self.datasource, RemoteSourceDest):
                self.set_source_dest(remote_url=self.datasource.url)
            else:
                self.set_source_dest(source_path=self.datasource.root,
                                     remote_url=self.datasource.url)
        return

    # gaining data from the catalog like json file `remote_sources.json`
    def load_remote_source_and_index(self):
        '''
        Load remote source path and necessary index files from `remote_sources.json`
        into static class variables.
        '''
        self.remote_url = self.REMOTE_SOURCE[self.DATABASE]["url"]
        self.index_url = self.REMOTE_SOURCE[self.DATABASE]["index"]["url"]
        self.index_files = self.REMOTE_SOURCE[self.DATABASE]["index"]["index_files"]
        self.features_possible = self.REMOTE_SOURCE[self.DATABASE]["features"]
        return
    
    
    # TODO make abstract method to check if data for location is defined
    def check_location(self, location):
        pass
    
    # allow to change tilesize
    def set_tile_size(self, tile_size):
        '''
        Change tile size of object ant hus for requests.
        '''
        self.tile_size = tile_size
        return
    
    
    # set database specific features
    def set_features(self, feature_list):
        '''
        Set the features that should be fetched for data retrieval.
        '''
        self.features = []
        for feature in feature_list:
            if feature.lower() in self.features_possible:
                self.features.append(feature.lower())
            else:
                raise RuntimeError(
                        f"Attention: Expected feature `{feature}`"
                        f" for database `{self.DATABASE}` was not found."
                        f" Use one of those: {self.features_possible}")
        return
        

    # change the url
    def set_remote_url(self, remote_url):
        '''
        Change the URL of the remote source.
        '''
        if remote_url is not None:
            self.remote_url = remote_url
            if not self.silent:
                print(f"URL of remote source for `{self.DATABASE}` was changed to `{self.remote_url}`.")
        return

    # initialize the csv file which is used to store metainformation
    def initialize_csvfile(self, header_dict, database_feature=None):
        '''
        Initialize a csv file that will serve as an index/meta_information
        storage for data fetched.
        '''
        # as the cache dir destination could lay outside of the destination, 
        # we treat this as a special case:
        if database_feature != "cache":
            csv_file_name = self.make_csv_name(
                self.datasource.destination.destination_path,
                database_feature
            )
        else:
            csv_file_name = self.make_csv_name(
                self.datasource.destination.cache_dir,
                ""
            ).replace("_.csv", "_cache.csv")
        
        write_csv_row(csv_file_name, header_dict)
        return csv_file_name

    # check if metainformation csv_file exists and return name
    def make_csv_name(self, database_path, database_feature):
        '''
        Initialize a csv file that will serve as an index/meta_information
        storage for data fetched.
        '''
        rel_csv_file_path = f"index_{self.DATABASE.lower().replace(' ', '_')}_{database_feature}.csv"

        if database_feature is None:
            database_feature = ""
        csv_file_path_base = os.path.join(
                database_path,
                database_feature
            )
        csv_file_name = os.path.join(csv_file_path_base, rel_csv_file_path)
        
        return csv_file_name
    
    # set the sourcedest class of the object
    def set_source_dest(self, source_path=None, remote_url=None, copy_local=True,
                       extend_local_cache=False):
        '''
        Set a local source for data extraction if neccessary files exists. 
        '''
        # TODO TEST IT!
        if source_path is not None:
            try:
                # if the source is not approved, a assertion error will be raised.
                self.datasource = LocalSourceDest(self, source_path, url=remote_url, copy_local=copy_local,
                                                 extend_local_cache=extend_local_cache)
            except AssertionError:
                pass
            else:
                return
        
        self.datasource = RemoteSourceDest(self, remote_url)
        return
    
    # construct a row for a csv metainformation file
    def make_csv_row(self, metainf_assembler, copied_row_as_list=None,
                     **kwargs):
        if copied_row_as_list is not None:
            assert  isinstance(copied_row_as_list, list), (
                "The copied row is not in the right format. Insert list.")
            assert len(copied_row_as_list) == len(metainf_assembler.HEADER), (
                f"The copied row ({len(copied_row_as_list)} elements) has an unequal "
                f"amount of columns as the header ({len(metainf_assembler.HEADER)} el.). "
            )
            csv_row = copied_row_as_list
        elif len(kwargs) > 0:
            csv_row = metainf_assembler.build_row(**kwargs)
        else:
            csv_row = [None] * len(metainf_assembler.HEADER)
        
        csv_row_dict = {hd: rw for hd, rw in
                zip(metainf_assembler.HEADER, csv_row)}
        return csv_row_dict
    

# metaclass to assemble csv files for metainformation
class MetAssembler(metaclass=ABCMeta):
    '''
    Base class for all dataminer specific csv assemblers
    '''
    def __init__(self):
        return

    @abstractmethod
    def build_row(**kwargs):
        '''
        Construct csv_row from kwargs.
        '''
        return

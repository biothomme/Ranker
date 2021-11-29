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

from data import cat

from utils import set_directory
from utils import download_to_path
from utils import check_locations_and_dates
from utils import write_csv_row

from reader import LocalReader
from reader import RemoteReader

# Core abstract class that provides important methods for all subclasses
class SpatialData(metaclass=ABCMeta):
    '''
    Base class for all spatial dataminers.
    '''
    # statically load the sources for all databases
    with open("remote_sources.json", "r") as rs_file:
        REMOTE_SOURCE = json.load(rs_file)
    
    # classic init
    def __init__(self, destination_path=None, remote_url=None, features=[],
                 source_path=None, copy_local=True, extend_local_cache=False,
                 silent=False):
        # prepare database dependent informations.
        self.load_remote_source_and_index()
        
        # print or no print?
        self.silent = silent
        
        # set root and database dir
        self.set_db_directory(destination_path)
        
        # change remote url if provided
        self.set_remote_url(remote_url)
        
        # option to load data from a local source
        # implemented with keeping `self.base_url`, to allow different query
        # architecture between local cache or online source
        self.set_reader(source_path=source_path, 
                        remote_url=self.remote_url,
                        copy_local=copy_local,
                        extend_local_cache=extend_local_cache)
        
        # define which datatypes should be fetched 
        ## TODO should be loaded:
        ## self.features = {"rgb": False, "ir": False}
        self.set_features(features)
        
        # to avoid overwriting by multidb wrapper we flag if destination was 
        # manually set by user.
        self.user_defined = False if destination_path is None else True
        
        # load the intake catalog function
        self.load_catalog()

        return
    
            
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
    def get_data(self, build_query, file_name, location, date_given=None):
        pass  # return data_retrieved

    def prepare(self):
        '''
        Prepare local directory to enable database specific download.
        '''
        # initialize db directory including a `source` subdirectory
        # which is important if the directory will later be used as
        # local source.
        if not os.path.exists(self.database_dir):  # TODO: self.database_dir is here not initialized
            print(f"The directory for database `{self.DATABASE}` will be"
                    " initialized.")
        
        # initiate the cache directory if neccessary
        set_directory(self.datasource.destination.cache_dir,
                      database_name=None)

        # subsequently we will collect filenames for csvs containing
        # metainformation of raw (source) data and the extracted 
        # feature specific data 
        self.csv_index_files = {}
        
        return

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
            data_files = self.get_data(queries, file_name, location, date_given=dt)
            
        return

    # set a directory for databaseminer class and store in self
    # this is important to allow flexibility in storing of
    # multiple dataminers
    def set_db_directory(self, directory, multi_db_access=False):
        # we avoid overwriting a userdefined destination
        # by the multibd wrapper
        if multi_db_access and self.user_defined:
            if not self.silent:
                print(f"For the database `{self.DATABASE}` "
                     "a used defined destination but not the "
                      "multi_db directory will be used.")
            return
        
        # setting new direcories
        self.root = set_directory(directory)
        self.database_dir = os.path.join(
                self.root,
                self.db_name)
        
        # after setting a new destination, the Reader objects need to be updated(if already set)
        if hasattr(self, "datasource"):
            if isinstance(self.datasource, RemoteReader):
                self.set_reader(remote_url=self.datasource.url)
            else:
                self.set_reader(source_path=self.datasource.root,
                                     remote_url=self.datasource.url)
        return

    # gaining data from the catalog like json file `remote_sources.json`
    def load_remote_source_and_index(self):
        '''
        Load remote source path and necessary index files from `remote_sources.json`
        into static class variables.
        '''
        self.get_std_name()
        
        self.remote_function = cat[self.db_name]
        self.remote_url = cat[self.db_name](filename="").urlpath.strip("/")
        self.index_url = cat[self.db_name].metadata["index_url"]
        self.index_files = cat[self.db_name].metadata["index_files"]
        self.features_possible = cat[self.db_name].metadata["features"]
        return
    
    
    # TODO make abstract method to check if data for location is defined
    def check_location(self, location):
        pass
    
    # allow to change tilesize
    def set_tile_size(self, tile_size):
        '''
        Change tile size of object and use for requests.
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
    def set_remote_url(self, remote_url=None):
        '''
        Change the URL of the remote source.
        '''
        if remote_url is not None:
            self.remote_url = remote_url.strip("/")
            if not self.silent : print(
                f"URL of remote source for `{self.DATABASE}` was changed to `{self.remote_url}`.")
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
                self.datasource.destination.destination_dir,
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
    
    # set the Reader class of the object
    def set_reader(self, source_path=None, remote_url=None, copy_local=True,
                       extend_local_cache=False):
        '''
        Set a Reader for data retrieval.
        
        If no `source_path` is provided, we use a RemoteReader.
        '''
        # with path is given we try to set LocalReader:
        if source_path is not None:
            try:
                # if the source is not approved, a assertion error will be raised.
                self.datasource = LocalReader(self, source_path, url=remote_url, copy_local=copy_local,
                                                 extend_local_cache=extend_local_cache)
            except AssertionError:
                pass
            else:
                if not self.silent : print(
                    f"For `{self.DATABASE}` access we use a LocalReader.")
                return
        
        # otherwise we use RemoteReader
        if not self.silent : print(
                f"For `{self.DATABASE}` access we use a RemoteReader.")
        self.datasource = RemoteReader(self, remote_url)
        return
    
    # construct a row for a csv metainformation file
    def make_csv_row(self, metainf_assembler, copied_row_as_list=None,
                     **kwargs):
        # this is for copied rows
        if copied_row_as_list is not None:
            assert  isinstance(copied_row_as_list, list), (
                "The copied row is not in the right format. Insert list.")
            assert len(copied_row_as_list) == len(metainf_assembler.HEADER), (
                f"The copied row ({len(copied_row_as_list)} elements) has an unequal "
                f"amount of columns as the header ({len(metainf_assembler.HEADER)} el.). "
            )
            csv_row = copied_row_as_list
        # this is to build a row from information stored in kwargs
        elif len(kwargs) > 0:
            csv_row = metainf_assembler.build_row(**kwargs)
        # this is to make the csv header
        else:
            csv_row = [None] * len(metainf_assembler.HEADER)
        
        csv_row_dict = {hd: rw for hd, rw in
                zip(metainf_assembler.HEADER, csv_row)}
        return csv_row_dict
    
    # simplifiy db name for indexing of catalog
    def get_std_name(self):
        '''
        Retrieve standardized name of the database (no blanks).
        '''
        self.db_name = self.DATABASE.lower().replace(" ", "_")
        return
    
    def load_catalog(self):
        '''
        Load database specific catalog.
        '''
        if not hasattr(self, "db_name"):
            self.get_std_name()
        self.cat = cat[self.db_name]
        return
    
# end SpatialData
    
    
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

# This file provides core utilities for the multiple datamining
# project on geolocations

from abc import ABCMeta, abstractmethod
import pickle
import rtree

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
        try:
            data_files = self.get_data(queries)
        except:
            print(f"It was not possible to fetch data for location {location}")
        else:
            print(f"Data for location {location} was downloaded as "
                    "{[df for df in data_files] if type(data_files) == list else datafiles}")
        return # WAS NOT TESTED YET!!!!

    # store the data 
    def store_data(self, data_retrieved):
        pass

    # check if data for location is defined
    def check_location(self, location):
        pass


# class for queries of the NAIP database
class NAIPData(SpatialData):
    # The code is strongly inspired by (#REF01):
    # https://planetarycomputer.microsoft.com/dataset/naip#Blob-Storage-Notebook
    # Thanks a lot to the authors.
    from multidb_wrapper import set_directory

    def __init__(self, tile_size=None, directory=None):
        self.database = "NAIP western europe Azure"
        self.root = set_directory(directory)
        self.database_dir = os.join(
                self.root,
                self.database.lower().replace(" ", "_"))


    def authenticate(self):
        # TODO # 
        print("logged in")


    def build_query(self, location, date=None):
        '''
        Construct a query for NAIP database from coordinates and date
        '''
        # the NAIP database used here can be accessed efficiently by first downloading
        # a tile index, which is used in a second step to retrieve the necessary tiles.
        # to load the index, we use a subsequent function.
        self.prepare()
        
        ## copied but adjusted code BEGIN ##
        # get tiles with overlap
        intersected_indices = list(self.tile_rtree.intersection(self.location.bounds))
        assert len(intersected_indices) > 0, (
                "Location has no intersections with NAIP tiles.")

        # initialize list of queries
        queries = []
        tile_intersection = False

        
        for idx in intersected_indices:
            intersected_file = self.tile_index[idx][0]
            intersected_geom = self.tile_index[idx][1]
            if intersected_geom.contains(point):
                # avoid that tiles only touch on edge/point
                tile_intersection = True
                queries.append(intersected_file)
        
        assert tile_intersection, (
                "Error: there are overlaps with tile index, "
                "but no tile completely contains selection")   
        assert len(queries) > 0, "Location has no intersections with NAIP tiles."
        if len(queries) == 1:
            return queries[0]
        else:
            return queries
        # copied but adjusted code END


    def get_data(self, queries):
        '''
        Download the data for the NAIP query.
        '''
        # deal with multiple queries for a single location
        if type(queries) == list:
            data_files = []
            for query in queries:
                data_files.append(self.get_data(query))
            # stitch m
            data_file = stitch_tiles(data_files)
        
        # deal with a single file; recursively
        else:
            data_file = query.replace("://", "_").replace("/", "_")
            try:
                urllib.request(query, data_file)
            except:
                print(f"Error, it was impossible to download data for the query {query}"
                        " at location {self.location}.")

        return data_file


    def prepare(self):
        '''
        Build NAIP database directory and download the index files if not already
        existent.
        '''
        INDEX_FILES = ["tile_index.dat", "tile_index.idx", "tiles.p"]
        # initialize db directory
        if not os.exists(self.database_dir):
            print(f"The directory for database '{self.database}' will be"
                    " initialized.")

        # download tile indices
        tile_indices_root = os.join(self.database_dir, "tile_indices")
        tile_indices_paths = [
                os.join(tile_indices_root, file) for file in INDEX_FILES]
        for i, tile_index_path in enumerate(tile_indices_paths):
            if not os.exists(tile_index_path):
                os.makedirs(tile_index_path)
                print(f"The tiles index file {tile_index_path}"
                        " ({i+1}/{len(INDEX_FILES}) was downloaded.")
        
        # load index_files (taken from #REF01)
        self.tile_rtree = rtree.index.Index(
                os.join(tile_indices_root, "tile_index"))
        self.tile_index = pickle.load(open(tile_indices_root[2], "rb"))
        return



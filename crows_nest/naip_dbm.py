# This file contains the class for queries of the NAIP database
# it uses the NAIP western europe Azure blob storage.

# package imports
import os
import pickle
import rtree


# local imports
from utils import set_directory
from utils import download_to_path
from database_classes import SpatialData


# the class
class NAIPData(SpatialData):
    # The code is strongly inspired by (#REF01):
    # https://planetarycomputer.microsoft.com/dataset/naip#Blob-Storage-Notebook
    # Thanks a lot to the authors.
    from utils import set_directory

    def __init__(self, tile_size=100, directory=None):
        self.database = "NAIP western europe Azure"
        self.base_url = "https://naipblobs.blob.core.windows.net/naip"
        self.root = set_directory(directory)
        self.database_dir = os.path.join(
                self.root,
                self.database.lower().replace(" ", "_"))
        self.tile_size = tile_size
        return

    def authenticate(self):
        # TODO # 
        print("logged in")
        return


    def build_query(self, date=None):
        '''
        Construct a query for NAIP database from coordinates and date
        '''
        # the NAIP database used here can be accessed efficiently by first downloading
        # a tile index, which is used in a second step to retrieve the necessary tiles.
        # to load the index, we use a subsequent function.
        self.prepare()
        
        ## copied but adjusted code BEGIN ##
        # get tiles with overlap
        queries = _get_intersected_tile(self.location.bounds, self.date,
                self.tile_rtree, self.tile_index)
        return queries


    def get_data(self, queries):
        '''
        Download the data for the NAIP query.
        '''
        # deal with multiple queries for a single location
        if type(queries) == list:
            data_files = []
            for query in queries:
                data_files.append(self.get_data(query))
            # stitch multiple tiles to one
            # TODO data_file = stitch_tiles(data_files)
            data_file = data_files[0] 
        # deal with a single file as @queries; recursively
        else:
            data_file = os.path.join(self.database_dir, queries)
            query_url = "/".join([self.base_url, queries]) 
            try:
                download_to_path(query_url, data_file, silent=False)
            except:
                print(f"Error, it was impossible to download data for the query"
                        f" '{queries}' at location {self.location}.")

        return data_file


    def prepare(self):
        '''
        Build NAIP database directory and download the index files if not already
        existent
        '''
        URL_INDEX = "https://naipeuwest.blob.core.windows.net/naip-index/rtree"
        INDEX_FILES = ["tile_index.dat", "tile_index.idx", "tiles.p"]
        # initialize db directory
        if not os.path.exists(self.database_dir):
            print(f"The directory for database '{self.database}' will be"
                    " initialized.")

        # download tile indices which are 3 files as in INDEX_FILES
        tile_indices_root = os.path.join(self.database_dir, "tile_indices")
        tile_indices_paths = {
                os.path.join(tile_indices_root, file):
                "/".join([URL_INDEX, file]) for file in INDEX_FILES}
        for i, (ti_path, ti_url) in enumerate(tile_indices_paths.items()):
            download_to_path(ti_url, ti_path)
        
        # load index_files (taken from #REF01)
        self.tile_rtree = rtree.index.Index(
                os.path.join(tile_indices_root, "tile_index"))
        self.tile_index = pickle.load(
                open([fl for fl in tile_indices_paths.keys()
                    if "tiles.p" in fl][0], "rb"))
        
        # there are multiple years where data was obtained. also, there are
        # multiple resolutions. as all of the tiles should reflect the same
        # physical size, tiles need to be adjusted by their pixel sizes.
        # we try to obtain that by calculating a tile size in pixel for each
        # combination of resolution and date (tuple). we store it in a
        # dictionary. this takes some steps
        # 1 - get all different tuples
        resolutions_and_years = set(  # TODO this using less space?
                map(_get_resolution_and_date,
                    [ix[0] for ix in self.tile_index.values()]))
        print(resolutions_and_years)

        # 2 - retrieve the number of pixels per tile dimension for each
        #     tuple
        #     the computation is standardized by using the centroid of 
        #     the rectangular bounds of the dataset.
        e, n, w, s = self.tile_rtree.bounds
        centroid = _get_centroid(e, n, w, s)
        
        self.tile_sizes_dict = 

        return


# helpers

# extract year and resolution from an index query
def _get_resolution_and_date(query):
    '''
    Given a query extract the resolution and year (date) of
    the image and return as tuple.
    '''
    import re
    from datetime import date
    
    # each query has the substring `cm_`, which is set
    # after the resolution and before the year.
    resolution = int(re.search(r'\d+(?=cm)', query).group(0))
    date_given = int(re.search(r'(?<=cm_)\d+', query).group(0))
    return (resolution, date(date_given, 1, 1))    

# assign tile size in pixels for a given resolution and 
# year tuple
def _get_tile_pixels(resolution_year_tuple):
    '''
    '''
    centroid = 
    return 

def _get_centroid(east_limit, north_limit,
        west_limit, south_limit):
    '''
    Infer the centroid point given limits in all cardinal
    directions.
    the coordinates should be entered in epsg:4326 system.
    '''
    import geopandas as gpd
    diagonal_line = gpd.GeoSeries(
            [LineString([
                [east_limit, north_limit],
                [west_limit, south_limit]])],
            crs="epsg:4326")
    # as we need a projected epsg to draw a flat line, the
    # line will be transferred to epsg:3857 (a projection)
    # to compute the centroid with a following backtrafo.
    diagonal_line_in_epsg3857 = diagonal_line.to_crs("epsg:3857")
    centroid = diagonal_line_in_epsg3857.centroid.to_crs("epsg:4326")
    return centroid

# obtain the tile filenames that fit a given location to best date
def _get_intersected_tiles(point, preferred_date, tile_rtree, tile_index,
        strict_date=False, no_date_filter=False):
    '''
    TODO
    '''
    intersected_indices = list(tile_rtree.intersection(point))
    assert len(intersected_indices) > 0, (
            "Location has no intersections with NAIP tiles.")

    # initialize list of queries
    queries = []
    tile_intersection = False
    
    # find the dataset of with the best fitting date.
    # it should be the older or as old than/as the date
    # requested, but the latest as possible.
    # if there is no dataset older than the requested
    # date we prefer to take the oldest.
    dates = [_get_resolution_and_date(tile_index[i][0])[1]
            for i in intersected_indices]
    if strict_date:  # allows to only use dates from the same year.
        dates.filter(lambda x: x.year == date_given.year)
    else if all(dates > date_given):
        best_date = min(dates)
    else:
        dates = dates.filter(lambda x: x <= date_given)
        best_date = max(dates)

    # load the tiles which overlap with the location
    for idx in intersected_indices:
        intersected_file = self.tile_index[idx][0]
        if (_get_resolution_and_date(intersected_file)[1] == best_date or
                no_date_filter):  # still allows to retrieve all intersecting tiles
            intersected_geom = self.tile_index[idx][1]
            if intersected_geom.contains(self.location):
                # avoid that tiles only touch on edge/corner
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

def _compute_tile_pixel(tile_size, query):


# because this database allows high resolution data, we try to
# download quadratic tiles of a given size.
def fetch_data_tile(query_url, data_file):
    date = ""

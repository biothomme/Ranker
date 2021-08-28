# This file contains the class for queries of the NAIP database
# it uses the NAIP western europe Azure blob storage.

# package imports
from datetime import date
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

    def __init__(self, tile_size=100, directory=None, date_given=None):
        self.database = "NAIP western europe Azure"
        self.base_url = "https://naipblobs.blob.core.windows.net/naip"
        self.root = set_directory(directory)
        self.database_dir = os.path.join(
                self.root,
                self.database.lower().replace(" ", "_"))
    
        if date_given is None:
            self.date = date.today()
        else:
            self.date = date_given
        
        self.tile_size = tile_size
        self.tile_sizes_dict = {}

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
        queries = _get_intersected_tiles(self.location, self.date,
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


    def prepare(self, silent=False):
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
        # combination of resolution and year (tuple). we store it in a
        # dictionary. this takes some steps
        if not self.tile_sizes_dict:
        # 1 - get all different tuples
            resolutions_and_years = set(  # TODO this using less space?
                    map(_get_resolution_and_date,
                        [ix[0] for ix in self.tile_index.values()]))
        
        # 2 - retrieve the mean number of pixels per tile. here we use
        #     for each resolution_year_tuple the first image in index order
        #     to calculate the numbers of pixels per tile (averaged between
        #     height and width).
            for ryt in resolutions_and_years:
                self.tile_sizes_dict[ryt] = _compute_tile_pixel(
                        self.tile_size, self.tile_index, ryt)
            if not silent:
                print(f"A size of {self.tile_size} meters was chosen for each",
                        "tile dimension. For different resolutions and",
                        "years, this corresponds to the following sizes",
                        "in pixels:")
                for ((r, y), size) in self.tile_sizes_dict.items():
                    print(f"    - {y.year} ({r} cm): {size} px") 
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


# obtain the tile filenames that fit a given location to best date
def _get_intersected_tiles(point, date_preferred, tile_rtree, tile_index,
        strict_date=False, no_date_filter=False):
    '''
    Look up tile file name(s) that intersect with a given point.
    These can be even selected for a given year (or best year before it).
    '''
    intersected_indices = list(tile_rtree.intersection(point.bounds))
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
        dates = filter(lambda x: x.year == date_preferred.year, dates)
    elif all(map(lambda x: x > date_preferred, dates)):
        best_date = min(dates)
    else:
        dates = filter(lambda x: x <= date_preferred, dates)
        best_date = max(dates)

    # load the tiles which overlap with the location
    for idx in intersected_indices:
        intersected_file = tile_index[idx][0]
        if (_get_resolution_and_date(intersected_file)[1] == best_date or
                no_date_filter):  # still allows to retrieve all intersecting tiles
            intersected_geom = tile_index[idx][1]
            if intersected_geom.contains(point):
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


# obtain tile size in pixels for a resolution_year_tuple.
# the first image in the index, sharing this tuple is
# used for the estimation
def _compute_tile_pixel(tile_size, tile_index, resolution_year_tuple):
    '''
    Compute the tile size in pixels for a given resolution
    and year.
    '''
    import numpy as np

    # we extract name and geom of the first image that is in given
    # resolution from given year. this is standardized, as index 
    # is ordered (as long as the index of the database does not 
    # change - who knows?...)
    for i in tile_index:
        if _get_resolution_and_date(
                tile_index[i][0]) == resolution_year_tuple:
            index_first_ryt = i
            break

    tile_query = tile_index[index_first_ryt][0]
    tile_geom = tile_index[index_first_ryt][1]
    
    # we compute physical and pixel width and height
    tile_query_url = _make_query_url(tile_query)
    dim_pixel = np.array(_fetch_image_dimensions(tile_query_url))
    dim_metric = np.array(_fetch_geom_dimensions(tile_geom))

    # then they can be used to compute a mean number of pixels per
    # meter.
    pixels_per_metres = np.mean(dim_pixel/dim_metric)
    
    # finally, multiplied by tilesize in metres this gives us the
    # number of pixels per tile. we use ceiling, so the tiles are 
    # as large as, or larger, than wanted.
    tile_size_pixels = int(np.ceil(pixels_per_metres*tile_size))
    # for symmetric data extraction even pixelsizes are preferred.
    if tile_size_pixels % 2 == 1:
        tile_size_pixels += 1 
    return tile_size_pixels


# build the mature url for query
def _make_query_url(query):
    '''
    Add database url to a given tile specific query.
    '''
    BASE_URL = "https://naipblobs.blob.core.windows.net/naip"
    query_url = "/".join([BASE_URL, query])
    return query_url

# retrieve dimensions (height, width) of image in pixels
def _fetch_image_dimensions(query_url):
    '''
    Fetch the height and width (pixel) of image given its url.
    tuple of (height, width) will be returned
    '''
    import rasterio
    with rasterio.open(query_url) as image:
        hw_tuple = (image.height, image.width)
    return hw_tuple

# retrieve dimensions (height, width) of geom_rectangle in metres
def _fetch_geom_dimensions(geom_rectangle):
    bounds = geom_rectangle.bounds
    from geopy.distance import geodesic
    from numpy import mean

    # as parallel edges of geodesic rectangles do not always have
    # the same length, we take the mean of the opposing sides.
    metric_height = mean([
        geodesic((bounds[1], bounds[0]), (bounds[3], bounds[0])).m,
        geodesic((bounds[1], bounds[2]), (bounds[3], bounds[2])).m])
    metric_width = mean([
        geodesic((bounds[3], bounds[0]), (bounds[3], bounds[2])).m,
        geodesic((bounds[1], bounds[0]), (bounds[1], bounds[2])).m])
    return (metric_height, metric_width)

# because this database allows high resolution data, we try to
# download quadratic tiles of a given size.
def fetch_data_tile(query_url, data_file):
    date = ""

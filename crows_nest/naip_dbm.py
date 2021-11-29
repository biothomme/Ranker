# This file contains the class for queries of the NAIP database
# it uses the NAIP western europe Azure blob storage.

## package imports ##
from datetime import date
from dateutil import parser
import numpy as np
import os
import pickle
import rtree


## local imports ##
from utils import coordinatify_point
from utils import download_to_path
from utils import make_csv_path
from utils import retrieve_image_info
from utils import set_directory
from utils import write_csv_row
from database_classes import SpatialData
from database_classes import MetAssembler

from image_manipulation import FileStitcher
from reader import LocalReader
from reader import RemoteReader


## the class ##
class NAIPData(SpatialData):
    # The code is strongly inspired by (#REF01):
    # https://planetarycomputer.microsoft.com/dataset/naip#Blob-Storage-Notebook
    # Thanks a lot to the authors.
    from utils import set_directory
    
    DATABASE = "NAIP western europe Azure"
    
    def __init__(self, features=[], tile_size=100, destination_path=None,
                 remote_url=None, source_path=None, date_given=None,
                 copy_local=True, extend_local_cache=False, silent=False):
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
        
        # remember, if db index was already fetched
        self.prepared = False

        # define which datatypes should be fetched 
        ## TODO should be loaded:
        ## self.features = {"rgb": False, "ir": False}
        self.set_features(features)
        
        # define tile metrics and init the stitcher.
        self.tile_size = tile_size
        self.tile_sizes_dict = {}
        self.tile_stitcher = FileStitcher(tile_size, features, silent=self.silent)
        
        # to avoid overwriting by multidb wrapper we flag if destination was 
        # manually set by user.
        self.user_defined = False if destination_path is None else True

        return

    def authenticate(self):
        # TODO # 
        print("logged in")
        return


    def build_query(self, location, date=None):
        '''
        Load the relative paths of a tile in a given location NAIP database from coordinates and date.
        '''
        # the NAIP database used here can be accessed efficiently by first downloading
        # a tile index, which is used in a second step to retrieve the necessary tiles.
        # to load the index, we use a subsequent function.
        if not self.prepared:
            self.prepare()
            self.prepared = True
        
        # get tiles with overlap
        rel_tile_paths = _get_intersected_tiles(location, date,
                self.tile_rtree, self.tile_index)
        return rel_tile_paths
    
    def get_local_src_dest_path(self, rel_tile_path):
        '''
        Obtain the relative path for a source data instance, how it should be located
        in a local cache.
        '''
        tile_path = os.path.join(self.datasource.cache_dir, rel_tile_path)
        return tile_path

    def get_remote_src_query(self, rel_tile_path):
        '''
        Obtain a query for a given remote source tile. 
        '''
        query_url = "/".join([self.datasource.url, rel_tile_path])
        return query_url
        
        
    def get_data(self, queries, file_name, location, date_given):
        '''
        Download the data for the NAIP query.
        '''
        ry_tuple = _get_resolution_and_date(queries[0])  # TODO is this actually right to do?
        
        # first download the whole image (or load from local source)
        raw_file_names = []
        for query in queries:
            try:
                dest_file_path, csv_row_dict = self.datasource.fetch_data(
                    query, NaipMetCacheAssembler)

            except PermissionError: # WHAT do i want to except? PermissionError? TODO 
                print(f"Error, it was impossible to download data for the query"
                        f" `{query}` at location `{coordinatify_point(location)}`. No permission.")
            else:
                # document the new data retrieved.
                if csv_row_dict is not None:
                    write_csv_row(self.csv_index_files["cache"], csv_row_dict)
            raw_file_names.append(dest_file_path)
        try:
            image_manipulation = self.tile_stitcher.stitch_image(
                    location, raw_file_names, file_name_prefix=file_name)
        except ValueError as err: # WHAT do i want to except TODO
            print(f"Error, it was impossible to stitch data for the queries"
                    f" '{queries}' at location {coordinatify_point(location)}. {err}")
            image_manipulation = None
        else:
            # TODO
            for feature in self.features:
                final_file_name = f"{file_name}.tif".replace(
                    "FEATURE_PLACE_HOLDER", feature)  # same as in image_manipulation.py
                csv_row_dict = self.make_csv_row(NaipMetFeatureAssembler,
                    location=location, date_requested=date_given, tile_size=self.tile_size,
                    file_name=final_file_name, manipulation_dict=image_manipulation[final_file_name]
                )
                write_csv_row(self.csv_index_files[feature], csv_row_dict)
        
        return


    def prepare(self):
        '''
        Build NAIP database directory and download the index files if not already
        existent
        '''
        # initialize db directory including a `source` subdirectory
        # which is important if the directory will later be used as
        # local source.
        if not os.path.exists(self.database_dir):
            print(f"The directory for database {self.DATABASE} will be"
                    " initialized.")

        # initiate the cache directory if neccessary
        set_directory(self.datasource.destination.cache_dir,
                      database_name="")

        # subsequently we will collect filenames for csvs containing
        # metainformation of raw (source) data and the extracted 
        # feature specific data 
        self.csv_index_files = {}

        # make first line in csv_file
        source_header_dict = self.make_csv_row(NaipMetCacheAssembler)
        self.csv_index_files["cache"] = self.initialize_csvfile(
            source_header_dict, database_feature="cache")

        # initialize aa directory for each feature
        for feature in self.features:
            set_directory(self.datasource.destination.destination_path,
                          database_name=feature)
            feature_header_dict = self.make_csv_row(NaipMetFeatureAssembler)
            self.csv_index_files[feature] = self.initialize_csvfile(
                feature_header_dict, database_feature=feature)
        
        # download tile indices which are 3 files as in self.index_files
        tile_indices_root = set_directory(
            self.datasource.destination.destination_path,
            database_name="index_files")

        for rel_index_file_path in self.index_files:
            index_file_path = os.path.join(tile_indices_root, rel_index_file_path)
            # we do not update already existing indices! TODO DO WE????
            if not os.path.exists(index_file_path):
                download_to_path(
                    "/".join([self.index_url, rel_index_file_path]),  # url
                    index_file_path  # path 
                )
        
        # load index_files (taken from #REF01)
        self.tile_rtree = rtree.index.Index(
            os.path.join(tile_indices_root, "tile_index"))
        self.tile_index = pickle.load(
            open(os.path.join(tile_indices_root, "tiles.p"), "rb"))
        
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
            self.tile_sizes_dict = _compute_tile_pixel_dict(
                self.tile_size, self.tile_index, resolutions_and_years, self.datasource)

            if not self.silent : print(
                f"INFO: A size of {self.tile_size} meters was chosen for each "
                "tile dimension. For different resolutions and "
                "years, this corresponds to the following sizes "
                "in pixels:")
                for ((r, y), size) in self.tile_sizes_dict.items():
                    print(f"    - {y.year} ({r} cm): {size} px") 
        return


    def make_file_name(self, index, total_number):
        '''
        Combine date and location to make a unique filename.
        '''
        today = str(date.today()).replace("-", "_")
        padding = total_number//10 + 2
        file_name = "/".join([self.database_dir, "FEATURE_PLACE_HOLDER",
            f"{today}_loc_{str(index+1).zfill(padding)}"])
        return file_name
## end class ##


## helpers ##

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
    # we define the "best fitting date" by simply
    # taking the nearest neighbour to the expected
    # one, no matter in which direction of time
    get_date_difference = lambda date_x, date_y: (
            np.abs((date_x - date_y).days)
            )

    dates = [_get_resolution_and_date(tile_index[i][0])[1]
            for i in intersected_indices]
    if strict_date:  # allows to only use dates from the same year.
        dates = filter(lambda x: x.year == date_preferred.year, dates)
    
    date_differences = {date_point: get_date_difference(date_point, date_preferred)
            for date_point in dates} 
    
    best_dates = [date_point for date_point, date_diff in date_differences.items()
            if date_diff == min(date_differences.values())]
    best_date = min(best_dates)  # by convention we choose the oldest date of all NNs.

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
    return queries
    # copied but adjusted code END


# obtain tile size in pixels for a resolution_year_tuple.
# the first image in the index, sharing this tuple is
# used for the estimation
def _compute_tile_pixel_dict(tile_size, tile_index, resolution_year_tuple_list,
        data_source):
    '''
    Compute the tile size in pixels for all tuples of resolution and year.
    '''
    # we extract name and geom of the first image that is in given
    # resolution from given year. this is standardized, as index 
    # is ordered (as long as the index of the database does not 
    # change - who knows?...)
    tile_sizes_dict = {ryt: None for ryt in resolution_year_tuple_list}

    for i in tile_index:
        ryt = _get_resolution_and_date(tile_index[i][0]) 
        # always run it for the first given tile providing an ryt
        if tile_sizes_dict[ryt] is None:
            tile_sizes_dict[ryt] = _compute_tile_pixel(
                tile_size, tile_index, i, data_source)
        if all([tv is not None for tv in tile_sizes_dict.values()]):
            break

    return tile_sizes_dict


def _compute_tile_pixel(tile_size, tile_index, first_tile_index, datasource):
    '''
    Compute the tile size in pixel for a file given its index.
    '''
    tile_query = tile_index[first_tile_index][0]
    tile_geom = tile_index[first_tile_index][1]

    # we compute physical and pixel width and height
    tile_query_url = datasource.fetch_data(
        tile_query, NaipMetCacheAssembler, dry_run=True)
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
def _make_query_url(query, source_base):
    '''
    Add database url to a given tile specific query.
    '''
    query_url = "/".join([source_base, query])
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


class NaipMetFeatureAssembler(MetAssembler):
    '''
    Class to assemble csv row including feature specific metadata
    for the extracted tiles.
    '''
    HEADER = [
            "location",  # coordinates of the request
            "date_requested",  # date which was requested
            "date_obtained",  # date of image capture (could be list)
            "tilesize",  # metric edgelength of tile
            "file_path",  # absolute path of the file
            "manipulation",  # information about processing of raw file(s)
            "completeness",  # tells if complete tile was retrieved
            "source",  # path(s)/url(s) of raw data used to obtain file
            "timestamp_create",  # timestamp of when file was created
            "file_type",  # format/type of the file
            "file_size",  # size of the file in bytes
            "image_size",  # size of image in pixels
            "image_mode"  # information about the image profile
    ]
    
    def build_row(location=None, date_requested=None, tile_size=None,
                  file_name=None, manipulation_dict=None):
        '''
        Build the feature specific row for a tile.
        '''
        import re

        REGEX = "[a-zA-Z]_[0-9]*_[a-zA-Z]{2}_[0-9]*_[0-9]{3}_[0-9]*"
        # dates of image capture are obtained from raw file names 
        raw_files = manipulation_dict["source_file"]
        substr_obtained = map(lambda x: x.split("_")[-1], re.findall(REGEX, raw_files))
        dates_obtained = map(lambda x: parser.parse(x).date(), substr_obtained)
        datestr_obtained =  "|".join([str(d) for d in dates_obtained])
        image_info_dict = retrieve_image_info(file_name) 

        csv_row = [  # same order as self.HEADER
            coordinatify_point(location),  # location
            str(date_requested),  # date_requested
            datestr_obtained,  # date_obtained
            tile_size,  # tilesize
            file_name,  # file_path
            manipulation_dict["manipulations"],  # manipulation
            manipulation_dict["completeness"],  # completeness
            raw_files,  # source
            image_info_dict["timestamp_created"],  # timestamp_create
            image_info_dict["format"],  # file_type
            image_info_dict["file_size"],  # file_size
            image_info_dict["pixel_size"],  # image_size
            image_info_dict["mode"]  # image_mode
        ]
        return csv_row
    
    
class NaipMetCacheAssembler(MetAssembler):
    '''
    Class to assemble csv row including source file metadata.
    '''
    HEADER = [
            "query_url",  # url of the single query
            "local_path",  # absolute path where file is stored locally
            "timestamp_server",  # timestamp of last mod on server
            "timestamp_fetched",  # timestamp of when file was downloaded
            "server",  # name of the server
            "file_type",  # format/type of the file
            "file_size"  # size of the file in bytes
    ]
    
    def build_row(query_url=None, file_name=None, meta_information_dict=None):
        '''
        Build the source specific row for a cache file.
        '''
        csv_row = [  # same order as self.HEADER
                query_url,  # query_url
                file_name,  # local_path
                str(parser.parse(
                    meta_information_dict["Last-Modified"])),  # timestamp_server
                str(parser.parse(
                    meta_information_dict["Date"])),  # timestamp_fetched
                meta_information_dict["Server"],  # server
                meta_information_dict["Content-Type"],  # file_type
                meta_information_dict["Content-Length"]  # file_size
        ]
        return csv_row

# this file contains helper functions for core parts of the package

import os
import shapely
import urllib

# helper for shapely initialization of coordinates
def set_locations(longitudes, latitudes):
    '''
    Create a list of shapely points for each pair of coordinates (longitude, latitude).
    '''
    LENGTH_ERR_MSG = (
        "The lists for latitudes ({len(longitudes)}) and "
        f"lon ({len(longitudes)}) have unequal length. Please adjust.")

    assert len(longitudes) == len(latitudes), LENGTH_ERR_MSG
    locations = [shapely.geometry.point.Point((lon, lat)) for lon, lat in zip(
        longitudes, latitudes)]

    return locations

# helper for initialization of directory
def set_directory(directory, database_name="data_barrel"):
    '''
    Set/Make a given directory and return it.
    If None is given, a temporary directory will be returned.
    '''
    import tempfile

    root = os.path.join(
            tempfile.gettempdir() if directory is None else directory,
            database_name)
    if not os.path.exists(root):
        os.makedirs(root)
        print(f"{'Temporal d' if directory is None else 'D'}irectory {root} was created.")

    return root

# helper to download a file from url and store it in a given path
def download_to_path(url, file_path,
        force=False, silent=True):
    '''
    Download data from a given URL and store it in a given (new) path.
    '''
    from urllib.error import HTTPError
    from urllib.error import URLError
    
    PATH_NOT_MADE_MSG = (f"It was not possible to create the given"
            f" path {file_path}.")
    # do not download if file already exists
    if (not os.path.exists(file_path) or force):
        # create parent directories if they are not existent
        parent_dir = os.path.dirname(file_path)
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir)
        assert os.path.exists(parent_dir), PATH_NOT_MADE_MSG
        
        # finally download the data
        print(url)
        print(file_path)
        try:
            urllib.request.urlretrieve(url, file_path)
        except (HTTPError, URLError) as err:
            print(f"The download of {url} failed with {err}")
        else:
            if not silent:
                print(f"Data from {url} was retrieved to {file_path}.")
    else:
        print(f"{file_path} already exists. It will not be "
            "overwritten.")
        return



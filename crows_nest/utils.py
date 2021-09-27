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
        force=False, silent=True, local_path=False):
    '''
    Download data from a given URL and store it in a given (new) path.
    
    The request header (metadata) will be retured as a dictionary.
    '''
    from urllib.error import HTTPError
    from urllib.error import URLError
    from shutil import copy2

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
        # TODO do we actually need this?
        if local_path:
            try:
                copy2(url, file_path)
            except:
                print(f"It was not possible to load {url} from local source."
                        if not silent else "", end="")
            else:
                print(f"Local data from {url} was loaded into {file_path}.\n"
                        if not silent else "", end="")
        else:
        # TODO end
            try:
                fp, headers = urllib.request.urlretrieve(url, file_path)
            except (HTTPError, URLError) as err:
                print(f"The download of {url} failed with {err}")
            else:
                print(f"Data from {url} was retrieved to {file_path}.\n"
                        if not silent else "", end="")
            return headers
    else:
        print(url)
        print(f"{file_path} already exists. It will not be "
            "overwritten.\n" if not silent else "", end="")
        return None


# helper to check if dates and locations list is of equal
# length. Also checks if dates are given at all.
# otherwise todays date will be used for all.
def check_locations_and_dates(locations, dates):
    '''
    Load dates to datetime object and assert that there is
    the same amount of locations and dates given.
    Assign most recent date for all locations if None
    provided
    '''
    from datetime import date

    if dates is not None:
        assert len(locations) == len(dates), (
                f"The length of the list of locations ({len(locations)})"
                f" does not align with the one of dates ({len(locations)})\n"
                f"Please check and try again.")
        dates = [d if type(d) == date else date(d) for d in dates]
    else:
        dates = [date.today()] * len(locations)
        print("As no dates were specified the most recent data will"
                " be fetched.")
    return dates


# we want to store metainformation about the data retrieved
# in a human readable format. thus, csv sheets should be used.
# with the csv package and a writer-buffer we can add lines
# synchronically when downloading a sample.
def write_csv_row(filename, row_dictionary):
    '''
    Store information about a sample in a csv file.

    Appending existing file needs to fulfill the criterium
    of congruent headers. If file does not exist, it will 
    be initialized.
    '''
    import csv
    
    # header can be retrieved from the row_dictionary
    header = row_dictionary.keys()

    if not os.path.exists(filename):
        with open(filename, "w") as csv_file:
            csv_writer = csv.DictWriter(
                    csv_file, fieldnames=header)
            csv_writer.writeheader()
        file_header = header
    else:
        # avoid to mess up different
        with open(filename, "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            file_header = csv_reader.__next__()
            if sorted(file_header) != sorted(header):
                raise RuntimeError(
                        f"The provided csv file {filename} already " 
                        "exists and does not have the same headers " 
                        f"({sorted(file_header)}) as the data given "
                        f"({sorted(header)}).")
    # buffer is always opened again, to avoid forgetting to close.
    with open(filename, "a") as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=file_header)
        csv_writer.writerow(row_dictionary)
    return
        
# helper for meta information files that track database requests etc.
def make_csv_path(base_path, database_name):
    '''
    Get standardized name for csv file that should store metainformation.
    '''
    file_path = os.path.join(
            base_path, "_".join([database_name, "index.csv"]))
    return file_path


# helper to get informations about an image:
def retrieve_image_info(image_file_path):
    '''
    Collect basic information about an image file and store in a dictionary.
    '''
    from PIL import Image
    image_info_dict = {}
    DICT_HEADER = ["format", "mode", "pixel_size", "timestamp_created", "file_size"]
    try:
        with Image.open(image_file_path) as image:
            image_info_dict["format"] = image.format
            image_info_dict["mode"] = image.mode
            image_info_dict["pixel_size"] = "x".join([str(x) for x in image.size])
    except IOError:
        image_info_dict = {info: "NA" for info in DICT_HEADER}
    try:
        image_info_dict["timestamp_created"] = os.path.getctime(image_file_path)
        image_info_dict["file_size"] = os.path.getsize(image_file_path)
    except IOError:
        pass
    return image_info_dict
    
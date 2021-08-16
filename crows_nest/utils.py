# this file contains helper functions for core parts of the package

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
        print(f"{'Temporal d' if directory is None else 'D'}irectory was created.")

    return root


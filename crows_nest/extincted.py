# this file serves as a fossil site for not needed functions,
# that could still be useful somewhen else

# CENTROID #
# get centroid of geodesic rectangle
def get_centroid(east_limit, north_limit,
        west_limit, south_limit):
    '''
    Infer the centroid point given limits in all cardinal
    directions.
    the coordinates should be entered in epsg:4326 system.
    '''
    import geopandas as gpd
    from shapely.geometry import LineString

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

# INVERSE HAVERSINE #
# get a point on earth, which has a given distance to a given location:
# retrieves coordinates of points that are normal/parallel to earth axes.
def inverse_haversine(point, distance=1000):
    '''
    Retrieve two points that are positioned in
    a given distance (meter) to given point.

    Input points need to be WGS84 geopandas point.
    Outputs limits in order: east (lon), north (lat),
    west (lon), south (lat)
    '''
    from math import asin
    from math import cos
    from math import degrees
    from math import radians
    from math import sin

    r = 6.371e6  # earth radius in meters at equator- TODO more accurate value!

    la = radians(point.y[0])  # latitude
    lo = radians(point.x[0])  # longitude
    d = distance

    # compute limits of longitudes with inverse haversine.
    #   inverse haversine was retrieved by solving earths haversine for
    #   one longitude value while fixing the distance and setting
    #   second latitude value equal to the first latitude value.
    lon_smd = 2 * asin( sin( d/(2*r) ))
    new_lons = [degrees(lo + smd) for smd in [lon_smd, -lon_smd]]
    east_limit = max(new_lons)
    west_limit = min(new_lons)


    # compute limits of longitudes with inverse haversine
    #   inverse haversine was retrieved by solving earths haversine for
    #   one latitude value while fixing the distance and setting
    #   second longitude value equal to the first longitude value.
    lat_smd = 2 * asin( sin( d/(2*r) / cos(lo)) )
    new_lats = [degrees(la + smd) for smd in [lat_smd, -lat_smd]]
    north_limit = max(new_lats)
    south_limit = min(new_lats)

    # test conversion
    for coord, lat_bool in zip(
        new_lons+new_lats, [False, False, True, True]):
        _test_inverse_haversine(point, coord, distance, latitude=lat_bool)

    return east_limit, north_limit, west_limit, south_limit


# test the accuracy of the conversion
def _test_inverse_haversine(point, new_value, distance, latitude=True):
    '''
    Test if the output points of the inverse haversine shows similar
    distance to the one expected.
    '''
    SINGIFICANT_FIGURES = 3

    from numpy.testing import assert_approx_equal
    from haversine import haversine

    lon = point.x
    lat = point.y

    # function varies if new coordinate is longitude or latitude
    if latitude:
        new_distance = 1000 * haversine((lon, lat), (lon, new_value))
    else:
        new_distance = 1000 * haversine((lon, lat), (new_value, lat))

    assert_approx_equal(new_distance, distance, significant=SINGIFICANT_FIGURES,
                        err_msg=("There was an internal problem while converting "
                                 "tile size to pixel size. It was not possible to "
                                 "accurately transform tile size to degree format."))
    return


# This function was part of NAIP before tiles were stitched...
# because this database allows high resolution data, we try to
# download quadratic tiles of a given size.
def fetch_best_tile(location, tile_size_pixels, rawdata_path,
                    file_name, features, silent=True):
    '''
    Download a tile of tile_size_pixels x tile_size_pixels of NAIP
    image centered around location, retrieved from query_url and
    to be stored as file_name.
    '''
    from fiona.transform import transform
    import numpy as np
    import rasterio

    IO_CRS = "epsg:4326"

    half_edge = int(tile_size_pixels/2)
    with rasterio.open(rawdata_path) as image:
        # here coordinates need to be shifted from the input crs to
        # the tif image crs and later converted to pixel
        location_in_img_crs = [p[0] for p in transform(
            IO_CRS, image.crs.to_string(), [location.x], [location.y])]
        location_in_img_pix = [
            int(np.floor(p)) for p in
            ~image.transform * location_in_img_crs]

        # a window around the tile can be produced
        wdw = rasterio.windows.Window(
            location_in_img_pix[0]-half_edge,
            location_in_img_pix[1]-half_edge,
            tile_size_pixels, tile_size_pixels)
        image_tile = image.read(window=wdw)

        kwargs = image.meta.copy()
        kwargs.update({
            'height': wdw.height,
            'width': wdw.width,
            'transform': rasterio.windows.transform(wdw, image.transform)})
        
        # store the tiles in rgb or ir image, if requested
        for profile, phot_prof in zip(["rgb", "ir"], ["RGB", "Grayscale"]):
            if features[profile]:
                try:
                    with rasterio.open(
                            file_name.replace("rgb", profile), "w",
                            photometric=phot_prof, **kwargs) as file:
                        if profile == "rgb":
                            file.write(image_tile)
                        else:  # TODO: how to save as greyscale image?
                            file.write(image_tile[3] , indexes=1)  # we only store the last layer.
                except IOError as err:
                    print(err)
                    print(f"It was not possible to store the {profile.upper()}"
                            f" image for tile at location {location}.\n" 
                            if not silent else "" )
                else:
                    print(f"{profile.upper()} image for tile at location" 
                            f" {location} was successfully downloaded to"
                            f" {file_name}.\n" if not silent else "", end="")
    return


# build the mature url for query
def _make_query_url(query, source_base):
    '''
    Add database url to a given tile specific query.
    '''
    query_url = "/".join([source_base, query])
    return query_url
# here one can find tools for image manipulation
import os
from shapely.geometry import Point
import cv2

## classes ##
# important class to stitch multiple images to one
# e.g. useful for NAIP data
class FileStitcher:
    '''
    Assembler of multiple files to one tile of given
    edgelength in pixel centered around a location.
    '''
    def __init__(self, tile_size_in_pixels, features):
        '''
        Construct with database features and 
        tile sizes given.
        '''
        self.tile_size = tile_size_in_pixels
        self.features = features
        self.stitcher = cv2.Stitcher_create()

        return
    
    
    def stitch_image(self, location, list_of_images: list, file_name_prefix=None):
        '''
        Extract and stitch (if neccessary) tile from image(s).
        '''
        from shutil import copyfile

        # we construct file name from location and tilesize.
        # onecould add date
        if file_name_prefix is None:
            filepath_prefix = os.path.join(
                os.getcwd(),
                f"{_stringisize_point(location)}_{self.tile_size}px")
        else:
            filepath_prefix = os.path.abspath(file_name_prefix)
        # first temporary tile_fragments are produced from all
        # raw images and stored feature specifically.
        print(list_of_images)
        temp_images = []
        stitch_images = []
        manipulations = {}
        for image_path in list_of_images:
            temp_images.append(
                self.make_temp_image(location, image_path))
        # then for each feature all images are merged
        for feature in self.features:
            final_image_name = f"{filepath_prefix}.tif".replace(
                    "FEATURE_PLACE_HOLDER", feature)
            manipulations[final_image_name] = []
            temp_images_feature = [
                loc_img[feature][0] for loc_img in temp_images]
            
            full_images = {li: ti[feature][0]
                for li, ti in zip(list_of_images, temp_images)
                           if ti[feature][1]
            }
            # single files do not need to be stitched
            # TODO: we would need a checker if all images have
            # required size
            if len(temp_images_feature) == 1:
                copyfile(temp_images_feature[0].name, final_image_name)
                manipulations[final_image_name].append("None")
            # we use a single file, if there is one with
            # the full size TODO sort by name
            elif len(full_images) > 0:
                print("full tile was given")
                chosen_rawfile = sorted(list(full_images.keys()))[0]
                chosen_tile_file = full_images[chosen_rawfile]
                copyfile(chosen_tile_file.name, final_image_name)
                manipulations[final_image_name].append(
                    f"chosen_file_{chosen_rawfile}")
            # stitch multiple tiles
            else:
                opened_images = [
                    cv2.imread(img.name)
                    for img in temp_images_feature]
                print([img.name for img in temp_images_feature])                
                (status, stitched_image) = self.stitcher.stitch(opened_images)
                print(status, stitched_image) 
                assert status == 0, (
                    f"Image stitching for location {location} "
                    "was not successful."
                )
                manipulations[final_image_name].append(f"stitched{len(opened_images)}")
                
                # some images do not fit the pixel dimensions anymore
                if (stitched_image.shape[0] != self.tile_size or
                    stitched_image.shape[0] != self.tile_size):
                    final_image = cv2.resize(stitched_image,
                                             (self.tile_size, self.tile_size))
                    manipulations[final_image_name].append(f"resized")
                else:
                    final_image = stitched_image

                cv2.imwrite(final_image_name, final_image)
                
            # lastly remove temporary images
            for temp_img in temp_images_feature:
                temp_img.close()
        
        return manipulations
    
    
    def make_temp_image(self, location, image_path):
        '''
        Extract the focal tile (often partial) from a given image and store it.
        '''
        from fiona.transform import transform
        from tempfile import NamedTemporaryFile
        import numpy as np
        import rasterio

        IO_CRS = "epsg:4326"

        file_name_suffix = "_".join([
            _stringisize_point(location),
            f"{self.tile_size}px"])
        file_name = image_path.replace(
            ".tif", f"{file_name_suffix}_FEATURE_PLACE_HOLDER")

        # we will work a lot with the half tile size
        half_edge = int(self.tile_size/2)
        
        with rasterio.open(image_path) as image:
            # here coordinates need to be shifted from the input crs to
            # the tif image crs and later converted to pixel
            location_in_img_crs = [p[0] for p in transform(
                IO_CRS, image.crs.to_string(),
                [location.x], [location.y])]
            location_in_img_pix = [
                int(np.floor(p)) for p in
                ~image.transform * location_in_img_crs]

            # a window around the tile can be produced
            rc = max(image.bounds[0], image.bounds[2])  # right corner
            bc = min(image.bounds[1], image.bounds[3])  # bottom corner
            borders = [int(np.floor(p)) for p in
                ~image.transform * [rc, bc]]

            # to avoid cutting a too large window, we define some edges
            # and lengths that help orienting
            wdw_left_edge = max([0, location_in_img_pix[0]-half_edge])
            wdw_top_edge = max([0, location_in_img_pix[1]-half_edge])
            wdw_right_edge = min([borders[0], location_in_img_pix[0]+half_edge])
            wdw_bottom_edge = min([borders[1], location_in_img_pix[1]+half_edge])

            wdw_height = min(
                [borders[1] - wdw_top_edge,  # dist btwn leftbrd and top wdw edge  
                 self.tile_size,  # tile size
                 wdw_bottom_edge]) # dist btwn rightbrd and bottom wdw edge  
            wdw_width = min(
                [borders[0] - wdw_left_edge,
                 self.tile_size, 
                 wdw_right_edge])

            wdw = rasterio.windows.Window(
                wdw_left_edge,
                wdw_top_edge,
                wdw_width,
                wdw_height)
            image_tile = image.read(window=wdw)
            kwargs = image.meta.copy()
            kwargs.update({
                'height': wdw.height,
                'width': wdw.width,
                'transform': rasterio.windows.transform(wdw, image.transform)})
                # store the tiles in rgb or ir image, if requested
            images_produced = {}
        for profile, phot_prof in zip(["rgb", "ir"], ["RGB", "Grayscale"]):
            if profile in self.features:
                full_size = (image_tile.shape[1] == self.tile_size and
                             image_tile.shape[2] == self.tile_size)
                print(full_size)
                try:
                    temp_image = NamedTemporaryFile(
                        mode="w",
                        prefix=file_name.replace("FEATURE_PLACE_HOLDER", profile),
                        dir=".",
                    suffix=".tif")
                    with rasterio.open(
                            temp_image.name, "w",
                            photometric=phot_prof, **kwargs) as file:
                        if profile == "rgb":
                            file.write(image_tile)
                        else:  # TODO: how to save as greyscale image?
                            file.write(image_tile[3] , indexes=1)  # we only store the last layer.
                except IOError as err:
                    pass
                else:
                    images_produced[profile] = (temp_image, full_size)

        return images_produced


# helpers
def _stringisize_point(shapely_point):
    '''
    Construct underscore seperated string from Point for filenames.
    
    If negative coordinates are given, we use m instead of minus.
    '''
    point_as_string = "_".join([
        str(shapely_point.y).replace(".", "_").replace("-", "m"), "N",
        str(shapely_point.x).replace(".", "_").replace("-", "m"), "E"])
    return point_as_string

def _check_if_image_has_tile_size(rasterio_image_wrapper, tile_edge_size):
    '''
    Check if an image tile has the actual tile_edge_size (as square).
    '''
    cv.imread(image)

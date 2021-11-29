from abc import ABCMeta
from abc import abstractmethod
import os
import urllib

from utils import download_to_path


class AbstractReader(metaclass=ABCMeta):
    def __init__(self, spatial_dataminer, url,
                 external_cache=None):
        self.dataminer = spatial_dataminer
        self.url = spatial_dataminer.remote_url if url is None else url
        self.destination = LocalDest(self.dataminer, cache_destination=external_cache)
        
        self.silent = self.dataminer.silent
        self.index_file_names = self.dataminer.index_files
        
        return

    
    # build the query
    @abstractmethod
    def fetch_data(self, source_file_query, met_assembler_csv, dest_file_path=None, force=False, dry_run=False):
        return  # return query


class RemoteReader(AbstractReader):
    def __init__(self, spatial_dataminer, url, external_cache=None):
        '''
        Generate new instance to retrieve data from remote source
        '''
        super().__init__(spatial_dataminer, url, external_cache=external_cache)
        if not self.silent : print(
            f"RemoteReader of the dataminer for `{self.dataminer.DATABASE}` "
            f"was set using the URL `{url}`")
        return

    
    def fetch_data(self, source_file_query, met_assembler_csv, dest_file_path=None, 
                   force=False, dry_run=False):
        '''
        Download data from a given query of a source file and store it in a given (new) path.
    
        The request header (metadata) will be retured as a dictionary.
        '''
        import urllib
        from urllib.error import HTTPError
        from urllib.error import URLError
        
        source_file_url = self.dataminer.get_remote_src_query(source_file_query)
        
        # only return query, do not fetch
        if dry_run:
            return source_file_url
        
        # build consistent filepath
        if dest_file_path is None:
            dest_file_path = self.destination.make_dest_file_path(
                self.dataminer.get_local_src_dest_path(source_file_query))

        # try the fetch
        if self.destination.prepare_filepath(dest_file_path, force=force):
            #try:
            file_xr = self.dataminer.cat(path_base=self.url, filename=source_file_query)
            #    print(file_xr, dest_file_path)
            #    file_xr.to_dask().rio.to_raster(dest_file_path)
            #    header = None
            # except ValueError:
            try:
                fp, header = urllib.request.urlretrieve(file_xr.urlpath, dest_file_path)
            except (HTTPError, URLError) as err:
                if not self.silent : print(
                    f"The download of `{source_file_url}` failed with {err}.")
                csv_dict = None
            else:
                csv_dict = self.dataminer.make_csv_row(met_assembler_csv, query_url=source_file_url,
                    file_name=dest_file_path, meta_information_dict=header)
                if not self.silent : print(
                    f"Data from `{source_file_url}` was retrieved to `{dest_file_path}`.")
        # if preparation fails we do not document it
        else:
            csv_dict = None
        return dest_file_path, csv_dict
    
    
    def store_index_files(self, dest_index_path=None):
        '''
        Download the index and save them in a given index directory.
        '''
        index_base_path = os.path.join(self.destination.destination_dir, "index")

        for rel_index_file_path in self.dataminer.index_files:
            index_file_path = os.path.join(index_base_path, rel_index_file_path)
            # we do not update already existing indices
            if not os.path.exists(index_file_path):
                source_index_path = "/".join([self.dataminer.index_url, rel_index_file_path])
                download_to_path(source_index_path, index_file_path, local_path=False)
                
        if not self.silent : print(
            f"All index files of `{self.dataminer.DATABASE}` were downloaded to "
            "their destination directory")
        return
# end RemoteReader

    
class LocalReader(AbstractReader):
    def __init__(self, spatial_dataminer, source_path, url=None, copy_local=True,
                extend_local_cache=False, external_cache=None):
        '''
        Generate new instance for local data retrieval and check for directories and indexfiles.
        
        The source directory must contain `cache` (and `index_files`) directories.
        If `copy_local` flag is on, file will be copied from local cache to local
        destination whenever a file is requested.
        '''
        super().__init__(spatial_dataminer, url, external_cache=external_cache)
        
        self.root = source_path
        self.remote_source = RemoteReader(self.dataminer, url)
        self.copy_local = copy_local
        
        cache_path = os.path.join(source_path, "cache")
        
        # we expect that overall database directory (source_path) and
        # cache_path exist
        if (os.path.exists(source_path) and os.path.exists(cache_path)):
            self.cache_dir = cache_path
            if (self.check_index_files() and 
                self.check_csv_index_file()):
                # we allow to set the source cache dir to
                # the destination cache_dir (external)
                if extend_local_cache:
                    self.remote_source.destination.cache_dir = self.cachedir
                    self.destination.cache_dir = self.cachedir
                if not self.silent : print(
                    f"RemoteReader of the dataminer for `{self.dataminer.DATABASE}` "
                    f"was set using the URL `{url}`")
                return
                
        # source path not existing -> remote source
        if not self.silent : print(
            f"The requested local database path `{source_path}` or its subdirectory "
            f"`{source_path}/source` for `{self.dataminer.DATABASE}` do not exist.\n"
            "The remote source will be used for data retrieval.")
        assert False, "Local source initialization failed."
        return
    

    def fetch_data(self, source_file_query, met_assembler_csv, 
                   dest_file_path=None, force=False, dry_run=False):
        '''
        Copy data from a given query of a source file if found in cache,
        otherwise the request is forwared to the remote source and finally
        stored in a given (new) path.

        The request header (metadata) will be retured as a dictionary.
        '''
        from shutil import copy2

        rel_data_path = self.dataminer.get_local_src_dest_path(source_file_query)
        source_file_path = os.path.join(self.cache_dir, rel_data_path)
        
        # only return query, do not fetch
        if dry_run:
            if os.path.exists(source_file_path):
                return source_file_path
            else:
                source_file_url = self.remote_source.fetch_data(
                    source_file_query, met_assembler_csv, 
                    dest_file_path=dest_file_path, force=force,
                    dry_run=True)
                return source_file_url

        # we need to ensure, that the data actually exists
        if os.path.exists(source_file_path):
            # one could define the path of the destination file oneself - not recommended
            if dest_file_path is None:  # TODO in practice this should be the case, to build a consistent hierarchy
                dest_file_path = self.destination.make_dest_file_path(
                    self.dataminer.get_local_src_dest_path(source_file_query))
            # do not download if dest_file already exists and prepare directories
            if self.destination.prepare_filepath(dest_file_path, force=force):
                if self.copy_local:
                    try:
                        file_xr = self.dataminer.cat(path_base=self.destination.cache_dir, file_name=path_base)
                        file_xr.to_dask().rio.rio.to_raster(dest_file_path)
                    except:
                        if not self.silent : print(
                            f"The copy process of `{source_file_path}` from LocalReader failed.")
                        csv_row = None
                    else:
                        csv_row = self.copy_csv_row(source_file_path, dest_file_path=dest_file_path) # TODO
                        if not self.silent : print(
                            f"Local data from `{source_file_path}` was copied to `{dest_file_path}`.")
            # for (1) existing files or (2) no `copy_local` flag,
            # we just use the same header.
                else: 
                    csv_row = self.copy_csv_row(source_file_path) # TODO
            else: 
                csv_row = None # TODO
            
            # building the csv dict from the simple row
            if header is not None:
                csv_dict = self.dataminer.make_csv_row(met_assembler_csv, copied_row=header)
            else:
                csv_dict = None
        # go remote with not existing cache data.
        else:
            dest_file_path, csv_dict = self.remote_source.fetch_data(source_file_query, met_assembler_csv, dest_file_path=dest_file_path,
                                                     force=force)
        return dest_file_path, csv_dict


    def check_index_files(self):
        '''
        Check if index files are existing.
        
        This is important to ensure a working local cache.
        '''
        # approve local source if no index files are required.
        if (self.dataminer.index_files is None or
            len(self.dataminer.index_files) == 0):
            return True

        # check the existence of neccesary index files (self.index_files).
        # otherwise we do not trust the local source
        index_files = [
                os.path.join(self.root, "index_files", ixfname) for
                ixfname in self.dataminer.index_files]
        if all([os.path.exists(ixf) for ixf in index_files]):
            if not self.silent: print(
                "Provided local data source has all neccessary index files "
                f"`{'`, `'.join(self.dataminer.index_files)}` for "
                f"`{self.dataminer.DATABASE}`.")
            return True
        # index not existing -> remote source
        else:
            if not self.silent: print(
                "Provided local data source does not have all of the neccessary index "
                f"files: `{'`, `'.join(self.dataminer.index_files)}`for "
                f"`{self.dataminer.DATABASE}`.\n"
                "A RemoteReader will be used for all data retrieval.")
            return False  # TODO does this work? (does it return a RemoteReader instance?)


    def check_csv_index_file(self):
        '''
        Check if the csv index file of the cache is existing.
        
        This is important to ensure a working local cache.
        '''
        csv_index_file = self.dataminer.make_csv_name(
            self.cache_dir, "").replace("_.csv", "_cache.csv")
        if os.path.exists(csv_index_file):
            self.csv_index_file = csv_index_file
            return True
        else:
            if not self.silent: print(
                "Provided local data source does not have the csv index "
                f"file `{csv_index_file}`, which stores metainformation for "
                f"`{self.dataminer.DATABASE}`.\n"
                "A RemoteReader will be used for all data retrieval.")
            return False
    
    
    def copy_csv_row(self, source_file_path, dest_file_path=None):
        '''
        Copy the row from the cache csv, which includes the source path of
        a file.
        '''
        csv_row = None
        with open(self.csv_index_file, "r") as csv_file:
            print(self.csv_index_file, source_file_path)
            lines = csv_file.readlines()
            for line in lines:
                if str(source_file_path) in line:
                    csv_row = line
                    break
        assert csv_row is not None, (
            "There was no corresponding row in the indexing "
            "csv_file of cache. Something is wrong.")
        csv_row_list = ",".split(csv_row)
        # we save the new file destination if given:
        if dest_file_path is not None:
            csv_row_list[csv_row_list == source_file_path] = dest_file_path
        
        return csv_row_list
    
    
    def store_index_files(self, dest_index_path=None):
        '''
        Download the index and save them in a given index directory.
        '''
        index_base_path = os.path.join(self.destination.destination_dir, "index")
        
        for rel_index_file_path in self.dataminer.index_files:
            index_file_path = os.path.join(index_base_path, rel_index_file_path)
            # we do not update already existing indices
            if not os.path.exists(index_file_path):
                source_index_path = os.path.join(self.root, "index", rel_index_file_path)
                download_to_path(source_index_path, index_file_path, local_path=True)
                
        if not self.silent : print(
            f"All index files of `{self.dataminer.DATABASE}` were downloaded to "
            "their destination directory")
        return
# end LocalReader


class LocalDest:
    '''
    The most important player in the orchestra: the conductor. Tells where stuff
    needs to be moved to.
    '''
    def __init__(self, dataminer, cache_destination=None):
        '''
        Initialize local destination object to organize storage of data.
        '''
        self.destination_dir = dataminer.database_dir
        
        # we divide between external cache and destination cache directory.
        # hence we allow to extend local cache directory (external).
        if cache_destination is not None:
            self.cache_dir = cache_destination
        else:
            self.cache_dir = os.path.join(self.destination_dir, "cache")
        
        self.silent = dataminer.silent
        if not self.silent : print(
                f"Destination of the dataminer for `{dataminer.DATABASE}` "
                f"was set to `{self.destination_dir}`, cache destination is in "
                f"`{self.cache_dir}`.")
        return
    
    
    def prepare_filepath(self, dest_file_path, force=False):
        '''
        Check if file already exists (and make parent directories).
        '''
        path_not_made_msg = (lambda x:
            f"It was not possible to create the given path `{x}`.")
        
        # do not download if file already exists
        if (not os.path.exists(dest_file_path) or force):
            # create parent directories if they are not existent
            parent_dir = os.path.dirname(dest_file_path)
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)
                if not self.silent : print(
                    f"Path to `{dest_file_path}` was prepared for file download.")
            assert os.path.exists(parent_dir), path_not_made_msg(parent_dir)
            return True
        
        else:
            if not self.silent : print(
                f"`{dest_file_path}` already exists. It will not be overwritten.")
            return False
    
    
    def make_dest_file_path(self, rel_source_file_path):
        '''
        Build a destination filepath for a query of a specified cache file.
        '''
        dest_file_path = os.path.join(self.cache_dir, rel_source_file_path)
        return dest_file_path
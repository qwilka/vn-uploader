
import collections
import copy
import datetime
import fnmatch
import logging
#import operator
import os
import pathlib
import shutil
import zipfile

logger = logging.getLogger(__name__)

import fs_meta
import utilities
from vn_tree import UploadNode, VnMeta



class UploadDataset(UploadNode):
    vn_uri = VnMeta()
    fs_path = VnMeta()
    fs_dev_uuid = VnMeta()

    def __init__(self, fs_path, name, vn_uri, meta):           
        super().__init__(name, None, meta)
        self._ppath = pathlib.Path(fs_path)
        # self._stg_ppath = pathlib.Path(vn_config.get_config("server", "stg_staging_dir"))
        # self._stg_ppath = self._stg_ppath / self.name
        if not fs_path or not self._ppath.exists():
            logger.error('%s: arg «fs_path» not specified correctly: «%s»' % (self.__class__.__name__, fs_path))
            return None
        self.fs_path = fs_path
        self.vn_uri = vn_uri
        self._id = utilities.string2UUID(self.vn_uri)
        self.db_uri = {
            "db": "visinum",
            "collection": "dataset",
            "_id": self._id,
        }
        self.ds_collection = "fs"            
        self.rootnode = self.make_fs_tree()
        self.set_dstree(self.rootnode, desc="Original file system tree.")
        self.rootnode = self.fs_tree_extend()
        self.set_dstree(self.rootnode, treename="vfs", desc="Extended fs, including zipped files.")


    def set_dstree(self, rootnode, treename="", desc=""):
        treedict = rootnode.to_treedict(full=False)
        _key = "ds_tree"
        if treename:
            _key = _key+"_"+treename
        self.set_data(_key, "desc", value=desc)
        self.set_data(_key, "treedict", value=treedict)
        self.set_data(_key, "vn_timestamp", value=datetime.datetime.now(datetime.timezone.utc))
        return True

    def get_dstree(self, treename=""):
        _key = "ds_tree"
        if treename:
            _key = _key+"_"+treename
        treedict = self.get_data(_key, "treedict")
        if not treedict:
            logger.warning('%s.get_tree: cannot find tree «%s»' % (self.__class__.__name__, _key))
            self.rootnode = None
            return None
        self.rootnode = UploadNode(treedict=treedict)
        return self.rootnode


    def make_fs_tree(self, parent=None, meta=None):
        _meta = collections.defaultdict(dict)
        if meta and isinstance(meta, dict):
            _meta.update(meta)
        _meta["fs"] = fs_meta.basic_fs_metadata(self.fs_path)
        rootnode = UploadNode(self._ppath.name, parent, data=_meta )
        for localdir, dirs, files in os.walk(self.fs_path, topdown=True):
            _loc_ppath = pathlib.Path(localdir)
            _loc_relppath = _loc_ppath.relative_to(self.fs_path)
            _parent = rootnode.get_nodepath(_loc_relppath.as_posix())
            if _parent:
                for dirname in dirs:
                    _dir_ppath =  _loc_ppath / dirname 
                    _meta["fs"] = fs_meta.basic_fs_metadata(_dir_ppath.as_posix(), self.fs_path)
                    UploadNode(_dir_ppath.name, _parent, _meta )
                for filename in files:
                    _file_ppath =  _loc_ppath / filename
                    _meta["fs"] = fs_meta.basic_fs_metadata(_file_ppath.as_posix(), self.fs_path)
                    UploadNode(_file_ppath.name, _parent, _meta )
        for _node in rootnode:
            _fs_path = _node.get_data("fs", "fs_path")
            _ppath = pathlib.Path(_fs_path)
            _relpath = _ppath.relative_to(self._ppath.parent).as_posix()
            _node.set_data("vn", "fs_relpath", value=_relpath) 
            _uri, _hash = utilities.make_fs_uri(_fs_path, 
                            fs_dev_uuid=self.fs_dev_uuid)
            _node.set_data("vn", "vn_uri", value=_uri)
            _node.set_data("fs", "fs_dev_uuid", value=self.fs_dev_uuid)
            _node.set_data("fs", "fs_uri", value=_uri)
            #_node.set_data("vn", "vn_uri_hash", value=_hash)
            _db_uri = {
                "db": "visinum",
                "collection": self.ds_collection,
                "_id": _hash,                
            }
            _node._id = _hash
            _node.set_data("vn", "db_uri", value=_db_uri)
            _node.set_data("vn", "vn_root_id", value=rootnode._id)
            _node.set_data("vn", "vn_timestamp", value=datetime.datetime.now(datetime.timezone.utc)) 
            #_node.set_data("vn", "fs_cat", value=_node.get_data("fs", "fs_cat")) # "vn_filetype"
            _node.set_data("vn", "ast_uri", value=None)
            _node.set_data("vn", "vn_cat", value="fs")
            #_node.set_data("vn", "vn_filetype", value=_vn_filetype)
            _node.set_data("vn", "vn_transform", value=dict())
        return rootnode

    def fs_tree_extend(self):
        treedict = self.get_data("ds_tree", "treedict")
        _vfs_root = UploadNode(treedict=treedict)
        for _node in _vfs_root:
            if _node.name.lower().endswith(".zip"):
                _rel_ppath = pathlib.Path(_node.get_data("vn", "fs_relpath"))
                # _zip_stg_dir = self._stg_ppath / _rel_ppath.parent / (_rel_ppath.stem + "__ZIPDIR")
                # #os.makedirs(_zip_stg_dir, mode=0o755, exist_ok=True)

                _zfile_vn_uri = _node.get_data("vn", "vn_uri")
                _zfile_ppath = self._ppath.parent / _rel_ppath 
                _valid_zip = False
                try:
                    _valid_zip = zipfile.is_zipfile(str(_zfile_ppath))
                except zipfile.BadZipFile as err:
                    logger.error("FsDataset.vfs_extend_import: cannot process zipfile «%s»; %s" % (str(_zfile_ppath),err))
                if _valid_zip:
                    _zipf = zipfile.ZipFile(str(_zfile_ppath))
                    for _info in _zipf.infolist():
                        _zf_ppath = pathlib.Path(_info.filename)
                        #print(_info.is_dir(), _zf_ppath, _zf_ppath.name)
                        _uri, _hash = utilities.make_fs_uri(path2=str(_zf_ppath), uri=_zfile_vn_uri)
                        #print(_uri, _hash)
                        if str(_zf_ppath.parent)=='.':
                            _zf_node = UploadNode(_zf_ppath.name, _node, None )
                        else:
                            _zparent = _node.get_nodepath(str(_zf_ppath.parent))
                            _zf_node = UploadNode(_zf_ppath.name, _zparent, None )
                        _zf_node._id = _hash
                        _zf_node.db_uri = copy.copy(_node.db_uri)
                        _zf_node.db_uri["_id"] = _hash
                        _zf_node.set_data("vn", "vn_uri", value=_uri)
                        _zf_node.set_data("vn", "vn_cat", value="fs_zfs")
                        #_zf_node.set_data("vn", "vn_uri_hash", value=_hash)
                        _vn_root_id = _node.get_data("vn", "vn_root_id")
                        _zf_node.set_data("vn", "vn_root_id", value=_vn_root_id)
                        _fs_relpath = _node.get_data("vn", "fs_relpath")
                        _zf_node.set_data("vn", "fs_relpath", value=_fs_relpath)
                        _zf_node.set_data("vn", "zip_root_id", value=_node._id)
                        _zf_node.set_data("vn", "zip_relpath", value=str(_zf_ppath))
                        if _info.is_dir():
                            _zf_node.set_data("vn", "fs_cat", value="zip_folder")
                            #print("DIR", _zf_ppath, _zf_ppath.parent, _node.path)
                        else:
                            _zf_node.set_data("vn", "fs_cat", value="zip_file")
                            #print("FILE", _zf_ppath, _zf_ppath.parent)
                        _zf_node.set_data("vn", "vn_timestamp", value=datetime.datetime.now(datetime.timezone.utc))
                        _zf_node.set_data("vn", "vn_transform", value=dict())

        #list(map(operator.methodcaller('db_update', timestamp=False), _vfs_root))
        #print(_vfs_root.to_texttree())
        self.set_dstree(_vfs_root, treename="vfs", desc="Extended FS tree, with zip file contents.")
        self.rootnode = _vfs_root
        return _vfs_root




if __name__=="__main__":
    fs_path = "/home/develop/Downloads/data/L51_dataset_testing"
    meta = {
        "testprop": 124,
    }
    ds = UploadDataset(fs_path, "test-dataset-Wupload", "test_uri", meta)
    print(ds.rootnode.to_texttree())


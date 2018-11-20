
import collections
import copy
import datetime
import fnmatch
#import json
import logging
#import operator
import os
import pathlib
import shutil
import zipfile

logger = logging.getLogger(__name__)


#from vntree import Node, TreeAttr


from . import fs_meta
from . import gdr_client
from . import vn_config
from . import vn_utilities
from . import uri
from .tree import UploadNode, NodeAttr
#from .vn_tree import UploadNode, VnMeta





class UploadDataset(UploadNode):
    #vn_uri = NodeAttr("vn")
    fs_path = NodeAttr("vn")
    fs_dev_uuid = NodeAttr("vn")
    itm_collection = NodeAttr("vn")

    def __init__(self, name, fs_path, collection_id, meta):
        if not isinstance(meta, dict) or "state" not in meta:
            raise ValueError("{}.__init__: arg «meta»=«{}», must be dict containing «state» item.".format(self.__class__.__name__, meta))
        self._ppath = pathlib.Path(fs_path)
        if not fs_path or not self._ppath.exists():
            raise IOError("{}.__init__: arg «fs_path», cannot find «{}»".format(self.__class__.__name__, fs_path))
        super().__init__(name, None, meta)
        # self._stg_ppath = pathlib.Path(vn_config.get_config("server", "stg_staging_dir"))
        # self._stg_ppath = self._stg_ppath / self.name
        self.fs_path = fs_path
        self.fs_dev_uuid = fs_meta.dirDev2UUID(fs_path)
        self._id = uri.state2uuid(self.data.get("state"))
        self.vn_uri = "{}_{}".format(self.name, self._id)
        self.db_uri = {
            "db": vn_config.get_config("database", "db"),
            "collection": "dataset",
            "_id": self._id,
        }
        self.itm_collection = "fs"
        self.rootnode = None        
        # self.rootnode = self.make_fs_tree()
        # self.set_dstree(self.rootnode, desc="Original file system tree.")
        # self.gdr_upload()
        #self.rootnode = self.fs_tree_extend()
        #self.set_dstree(self.rootnode, treename="zfs", desc="Extended fs, including zipped files.")
        # self.db_insert()
        # self.rootnode.db_insert(recursive=True)


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
            _parent = rootnode.get_node_by_nodepath(_loc_relppath.as_posix())
            if _parent:
                for dirname in dirs:
                    _dir_ppath =  _loc_ppath / dirname 
                    _meta["fs"] = fs_meta.basic_fs_metadata(_dir_ppath.as_posix(), self.fs_path)
                    UploadNode(_dir_ppath.name, _parent, _meta )
                for filename in files:
                    _file_ppath =  _loc_ppath / filename
                    _meta["fs"] = fs_meta.basic_fs_metadata(_file_ppath.as_posix(), self.fs_path)
                    UploadNode(_file_ppath.name, _parent, _meta )
        _basestate = copy.copy(self.data.get("state", {}))
        _basestate["statespace"] = "fs"
        for _node in rootnode:
            _fs_path = _node.get_data("fs", "fs_path")
            _ppath = pathlib.Path(_fs_path)
            _relpath = _ppath.relative_to(self._ppath.parent).as_posix()
            _node.set_data("vn", "fs_relpath", value=_relpath) 
            # _uri, _hash = vn_utilities.make_fs_uri(_fs_path, 
            #                 fs_dev_uuid=self.fs_dev_uuid)
            # _node.set_data("vn", "vn_uri", value=_uri)
            _node.set_data("fs", "fs_dev_uuid", value=self.fs_dev_uuid)
            _state = copy.copy(_basestate)
            _state["fs_path"] = _fs_path
            _state["fs_dev_uuid"] = self.fs_dev_uuid
            _node.set_data("state", value=_state)
            _hash = uri.state2uuid(_state)
            #_node.set_data("fs", "fs_uri", value=_uri)
            #_node.set_data("vn", "vn_uri_hash", value=_hash)
            _db_uri = {
                "db": vn_config.get_config("database", "db"),
                "collection": self.itm_collection,
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

    def fs_tree_extend(self, treename=None):
        if not self.rootnode:
            treedict = self.get_data("ds_tree", "treedict")
            self.rootnode = UploadNode(treedict=treedict)
        for _node in self.rootnode:
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
                    logger.error("UploadDataset.fs_tree_extend: cannot process zipfile «%s»; %s" % (str(_zfile_ppath),err))
                if _valid_zip:
                    _zipf = zipfile.ZipFile(str(_zfile_ppath))
                    for _info in _zipf.infolist():
                        _zf_ppath = pathlib.Path(_info.filename)
                        #print(_info.is_dir(), _zf_ppath, _zf_ppath.name)
                        ##_uri, _hash = vn_utilities.make_fs_uri(path2=str(_zf_ppath), uri=_zfile_vn_uri)
                        _state = copy.copy(_node.data.get("state", {}))
                        _state["statespace"] = "zfs"
                        _state["zip_path"] = str(_zf_ppath)
                        _hash = uri.state2uuid(_state)
                        #print(_uri, _hash)
                        if str(_zf_ppath.parent)=='.':
                            _zf_node = UploadNode(_zf_ppath.name, _node, None )
                        else:
                            _zparent = _node.get_node_by_nodepath(str(_zf_ppath.parent))
                            _zf_node = UploadNode(_zf_ppath.name, _zparent, None )
                        _zf_node._id = _hash
                        _zf_node.set_data("state", value=_state)
                        _zf_node.db_uri = copy.copy(_node.db_uri)
                        _zf_node.db_uri["_id"] = _hash
                        #_zf_node.set_data("vn", "vn_uri", value=_uri)
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
        if treename and isinstance(treename, str):
            self.set_dstree(self.rootnode, treename=treename, desc="Extended FS tree, with zip file contents.")
        return self.rootnode
    
    def gdr_upload(self, meta=None, clean=True):
        # if not collection_id:
        #     collection_id = self.get_data("gdr", "collection", "_id")
        if not self.rootnode:
            self.rootnode = self.make_fs_tree()
        if not meta:
            meta = {k:v for k,v in self.data.items() if not k.startswith("ds_tree")}
        _meta = vn_utilities.fix_JSON_datetime(meta)
        for _key in ["_id", "name", "desc"]:
            _meta.pop(_key, None)
        # if meta:
        #     _meta = utilities.fix_JSON_datetime(meta)
        # else:
        #     _meta = self.get_data()
        gc = gdr_client.get_girderclient()
        #collection_id = self.get_data("gdr", "collection", "_id")
        collection_id = self.get_data("gdr", "collection_id")
        #desc = getattr(self, "desc", "dataset")
        desc = self.get_data("description") or "dataset"
        ##print("collection_id", collection_id, self.name, desc)
        ds_folder = gc.createFolder(collection_id, self.name, desc, 
                    parentType="collection", reuseExisting=True, metadata=_meta)
        ##return None
        # delete folder contents (but retain folder), to refresh
        if clean:
            retVal = gc.delete("folder/{}/contents".format(ds_folder["_id"]) )
            print(retVal)

        def folder_callback(folder, fs_path):
            _pp = pathlib.Path(fs_path)
            ##nodepath = _pp.relative_to(self._ppath.parent).as_posix()
            nodepath = _pp.relative_to(self._ppath).as_posix()
            _node = self.rootnode.get_node_by_nodepath(nodepath)
            _uri, _hash = vn_utilities.make_fs_uri(fs_path, fs_dev_uuid=self.fs_dev_uuid)
            _gr_db_uri = {
                "db": "girder",
                "collection": "folder",
                "_id": folder['_id'],
            }
            _gdr_meta = {
                "collection": self.get_data("gdr", "collection"),
                "ds_folder_id": ds_folder["_id"],
                "db_uri": _gr_db_uri,
            }
            _node.set_data("gdr", value=_gdr_meta)
            _foldermeta = _node.get_data()
            for _key in ["_id", "name", "desc"]:
                _foldermeta.pop(_key, None)
            _foldermeta = vn_utilities.fix_JSON_datetime(_foldermeta)
            gc.addMetadataToFolder(folder['_id'], _foldermeta)

        def item_callback(item, fs_path):
            _pp = pathlib.Path(fs_path)
            ##nodepath = _pp.relative_to(self._ppath.parent).as_posix()
            nodepath = _pp.relative_to(self._ppath).as_posix()
            _node = self.rootnode.get_node_by_nodepath(nodepath)
            # print(_pp)
            # print(nodepath)
            _uri, _hash = vn_utilities.make_fs_uri(fs_path, fs_dev_uuid=self.fs_dev_uuid)
            _gr_db_uri = {
                "db": "girder",
                "collection": "item",
                "_id": item['_id'],
            }
            _file_ids = []
            for _child_file in gc.listFile(item["_id"]):
                _file_ids.append(_child_file["_id"])
            _gdr_meta = {
                "gdr_collection": self.get_data("gdr", "collection"),
                "ds_folder_id": ds_folder["_id"],
                "db_uri": _gr_db_uri,
                "gdr_file_id": _file_ids,
            }
            _node.set_data("gdr", value=_gdr_meta)
            _itemmeta = _node.get_data()
            for _key in ["_id", "name", "desc"]:
                _itemmeta.pop(_key, None)
            _itemmeta = vn_utilities.fix_JSON_datetime(_itemmeta)
            gc.addMetadataToItem(item['_id'], _itemmeta)

        gc.addFolderUploadCallback(folder_callback)
        gc.addItemUploadCallback(item_callback)
        retVal = gc.upload(self.fs_path, ds_folder["_id"], parentType='folder')
        return retVal



if __name__=="__main__":
    fs_path = "/home/develop/testdata/L51_dataset_testing"
    ast_uri = "NOR::SKARV::SUBSEA"
    svy_tag = "spl-ims"
    vn_date = "2015"

    ds_meta = {
        "name": "MBES_FVP",
        "desc": "Skarv 2015 IMS survey; MBES and 5-point data.",
        "ast": {
            "ast_uri": ast_uri,
        },
        "geo": {
            "EPSG": 23032,
            "SRS": "EPSG:23032",        
        },
        "vn": { 
            "vn_date": vn_date,
        },
        "svy": { 
            "vn_date": vn_date,
            "svy_tag": svy_tag,
            "svy_domain": ast_uri,
        },
    }

    coll_uri, _ = vn_utilities.make_survey_uri(ast_uri=ast_uri, svy_tag=svy_tag, vn_date=vn_date)
    collection = gdr_client.get_collection(coll_uri, desc="test create new collection")
    print(collection)
    ds_meta["gdr"] = {
        "collection" : {
            "_id": collection["_id"],
            "name": collection["name"],
        },
    }
    ds_name = ds_meta["name"]
    vn_uri, _ = vn_utilities.make_survey_uri(ast_uri=ast_uri, svy_tag=svy_tag, vn_date=vn_date, vn_cat="ds", location=ds_name)
    ds = UploadDataset(fs_path, coll_uri, vn_uri, ds_name, ds_meta)


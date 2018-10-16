
import copy
#import datetime
#import json
import logging
import os

logger = logging.getLogger(__name__)

from . import fs_meta
from . import vn_config

import girder_client


HTTPError = girder_client.HttpError


cache_gc = None


def initiailize(conf=None):
    app_config = vn_config.initiailize(conf)
    try:
        get_girderclient()
    except Exception as err:
        logger.error("initiailize: failed with error «%s»." % (err,) )
        return False
    # gc = get_girderclient()
    # if not gc:
    #     logger.error("initiailize: failed.")
    #     return False
    return True


def get_girderclient():
    global cache_gc
    if cache_gc:
        print("*** cache_gc ***")
        return cache_gc
    else:
        _config = vn_config.app_config
        gc = girder_client.GirderClient(apiUrl=_config["girder"]["apiUrl"])
        gc.authenticate(apiKey=_config["girder"]["apiKey"])
        # try:
        #     gc = girder_client.GirderClient(apiUrl=_config["girder"]["apiUrl"])
        #     gc.authenticate(apiKey=_config["girder"]["apiKey"])
        # except Exception as err:
        #     logger.error("get_girderclient: failed with error «%s»." % (err,) )
        #     return False
        cache_gc = gc
        return gc


def get_collection(name, description=""):
    gc = get_girderclient()
    tgt_coll = None
    for _coll in gc.listCollection():
        print("get_collection: found «{}»".format(_coll["name"]))
        if _coll["name"] == name:
            tgt_coll = _coll
            break
    if not tgt_coll:
        tgt_coll = gc.createCollection(name=name, description=description)
    return tgt_coll



def delete_folder(folder_id):
    gc = get_girderclient()
    retVal = gc.delete("folder/{}".format(folder_id) )
    #retVal = gc.delete("folder/{}/contents".format(folder_id) )
    return retVal



def upload_fs_dataset(fs_root_dir, collection_id,  
                    meta=None, dryRun=False, reuseExisting=True):
    # girder_obj = vn_config.get_girder_dct()
    # api_url = girder_obj["apiUrl"] 
    # api_key = girder_obj["apiKey"]
    gc = get_girderclient()
    if meta:
        _meta = copy.deepcopy(meta)
    else:
        _meta = {}
    fs_dev_uuid = fs_meta.dirDev2UUID(fs_root_dir)
    # ds_folder = create_folder(collection_id, _meta.get("name", "Dataset: "+os.path.basename(fs_root_dir)), 
    #     api_url, api_key,
    #     parentType='collection', 
    #     description=_meta.get("desc", "Data from: "+ fs_root_dir), 
    #     reuseExisting=True,
    #     meta=_meta)
    ds_folder = gc.createFolder(collection_id, 
        _meta.get("name", "Dataset: "+os.path.basename(fs_root_dir)), 
        _meta.get("desc", "Data from: "+ fs_root_dir), 
        parentType='collection',
        public=None, 
        reuseExisting=reuseExisting, 
        metadata=_meta)
    ds_folder_id = str(ds_folder["_id"])
    # _gr_ds_root_id = None
    # _vn_db_uri = vn_config.get_db_uri("visinum", "ofs")

    # def folder_callback(folder, fs_path):
    #     nonlocal _gr_ds_root_id 
    #     if fs_path==fs_root_dir:
    #         _gr_ds_root_id = folder["_id"] # NOTE not working, (may be set too late)
    #     _uri, _hash = vn_URI.make_vn_fs_URI(fs_path, fs_dev_uuid=fs_dev_uuid)
    #     _gr_db_uri = {
    #         "db": "girder",
    #         "collection": "folder",
    #         "_id": folder['_id'],
    #     }
    #     returnVal = vn_mongo.db_operation(_vn_db_uri, 'update_one', 
    #         filter={"_id":_hash}, 
    #         update={"$set": {"gdr.db_uri": _gr_db_uri} } )
    #     _vnmeta = vn_mongo.find_by_id(_vn_db_uri, _hash)
    #     _foldermeta = {
    #         "vn": json_ready_dct(_vnmeta["vn"]),
    #         "gdr": {
    #             "collection_id": collection_id,
    #             "ds_folder_id": ds_folder_id,
    #             "ds_root_id": _gr_ds_root_id,
    #             "db_uri": _gr_db_uri,
    #         },
    #     }
    #     gc.addMetadataToFolder(folder['_id'], _foldermeta)
    def folder_callback(folder, fs_path):
        _foldermeta = {
            "vn": _meta.get("vn", {}),
            "gdr": {
                "collection_id": collection_id,
                "ds_folder_id": ds_folder_id,
            },
        }
        gc.addMetadataToFolder(folder['_id'], _foldermeta)

    # def item_callback(item, fs_path):
    #     nonlocal _gr_ds_root_id
    #     _uri, _hash = vn_URI.make_vn_fs_URI(fs_path, fs_dev_uuid=fs_dev_uuid)
    #     _gr_db_uri = {
    #         "db": "girder",
    #         "collection": "item",
    #         "_id": item['_id'],
    #     }
    #     # returnVal = vn_mongo.db_operation(_vn_db_uri, 'update_one', 
    #     #     filter={"_id":_hash}, 
    #     #     update={"$set": {"gdr.db_uri": _gr_db_uri} } )
    #     _vnmeta = vn_mongo.find_by_id(_vn_db_uri, _hash)
    #     if not _vnmeta:
    #         print("ERROR in item_callback", fs_path, _hash)
    #     _filemeta = {
    #         "vn": json_ready_dct(_vnmeta["vn"]),
    #         "gdr": {
    #             "collection_id": collection_id,
    #             "ds_folder_id": ds_folder_id,
    #             "ds_root_id": _gr_ds_root_id,
    #             "db_uri": _gr_db_uri,
    #         },
    #     }
    #     _child_file = list(gc.listFile(item["_id"]))[0]
    #     if _child_file:
    #         _filemeta["gdr"]["gdr_file_id"] = _child_file["_id"]
    #     returnVal = vn_mongo.db_operation(_vn_db_uri, 'update_one', 
    #         filter={"_id":_hash}, 
    #         update={"$set": {"gdr": _filemeta["gdr"]} } )
    #     gc.addMetadataToItem(item['_id'], _filemeta)
    def item_callback(item, fs_path):
        _filemeta = {
            "vn": _meta.get("vn", {}),
            "gdr": {
                "collection_id": collection_id,
                "ds_folder_id": ds_folder_id,
            },
        }
        gc.addMetadataToItem(item['_id'], _filemeta)


    gc.addFolderUploadCallback(folder_callback)
    gc.addItemUploadCallback(item_callback)
    gc.upload(fs_root_dir, ds_folder_id, parentType='folder', 
        dryRun=dryRun, reuseExisting=reuseExisting)
    # retObj = {
    #     "ds_folder_id": ds_folder_id,
    #     "ds_root_id": _gr_ds_root_id,
    # }
    return retObj







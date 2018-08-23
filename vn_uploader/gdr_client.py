
#import copy
#import datetime
#import json
import logging
#import os

logger = logging.getLogger(__name__)

import utilities

import girder_client


HTTPError = girder_client.HttpError




def get_girderclient():
    # _config = utilities.get_config()
    # host = _config["girder"]["host"]
    # port = _config["girder"]["port"]
    # apiRoot = _config["girder"]["apiRoot"]
    # api_key = _config["girder"]["api_key"]
    # gc = girder_client.GirderClient(host=host, port=port, apiRoot=apiRoot)
    _config = utilities.app_config
    gc = girder_client.GirderClient(apiUrl=_config["girder"]["apiUrl"])
    gc.authenticate(apiKey=_config["girder"]["apiKey"])
    return gc


def get_collection(vn_uri, desc=""):
    gc = get_girderclient()
    tgt_coll = None
    for _coll in gc.listCollection():
        print(_coll)
        if _coll["name"] == vn_uri:
            tgt_coll = _coll
            break
    if not tgt_coll:
        tgt_coll = gc.createCollection(name=vn_uri, description=desc)
    return tgt_coll







def delete_folder(folder_id):
    gc = get_girderclient()
    retVal = gc.delete("folder/{}".format(folder_id) )
    #retVal = gc.delete("folder/{}/contents".format(folder_id) )
    return retVal



# def upload_fs_dataset(fs_dataset, meta=None, clean=True):
#     if meta:
#         _meta = fix_JSON_datetime(meta)
#     else:
#         _meta = None
#     gc = get_girderclient()
#     collection_id = self.get_data("gdr", "collection", "_id")
#     ds_folder = gc.createFolder(collection_id, "test foldername", "test description", 
#                 parentType="collection", reuseExisting=True, metadata=_meta)
#     # delete folder contents (but retain folder), to refresh
#     if clean:
#         retVal = gc.delete("folder/{}/contents".format(ds_folder["_id"]) )
#         print(retVal)
#     retVal = gc.upload(fs_path, ds_folder["_id"], parentType='folder')
#     return retVal







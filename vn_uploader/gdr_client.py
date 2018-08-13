
import copy
import datetime
import json
import logging
import os

logger = logging.getLogger(__name__)


import girder_client
import toml

HTTPError = girder_client.HttpError



def get_vn_config(conf_filepath="vn_config.toml"):
    with open(conf_filepath, 'r') as conf_fh:
        _config = toml.load(conf_fh)
    return _config

def get_girderclient():
    _config = get_vn_config()
    host = _config["girder"]["host"]
    port = _config["girder"]["port"]
    apiRoot = _config["girder"]["apiRoot"]
    api_key = _config["girder"]["api_key"]
    gc = girder_client.GirderClient(host=host, port=port, apiRoot=apiRoot)
    gc.authenticate(apiKey=api_key)
    return gc



def fix_JSON_datetime(dct):
    # https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable
    #_jsonstr = json.dumps(dct, default=bson.json_util.default)
    _jsonstr = json.dumps(dct, default=str)
    return json.loads(_jsonstr)




def delete_folder(folder_id):
    gc = get_girderclient()
    retVal = gc.delete("folder/{}".format(folder_id) )
    #retVal = gc.delete("folder/{}/contents".format(folder_id) )
    return retVal



def upload_fs_dataset(fs_path, collection_id, meta=None, clean=True):
    _meta = fix_JSON_datetime(meta)
    gc = get_girderclient()
    ds_folder = gc.createFolder(collection_id, "test foldername", "test description", 
                parentType="collection", reuseExisting=True, metadata=_meta)
    # delete folder contents (but retain folder), to refresh
    if clean:
        retVal = gc.delete("folder/{}/contents".format(ds_folder["_id"]) )
        print(retVal)
    retVal = gc.upload(fs_path, ds_folder["_id"], parentType='folder')
    return retVal





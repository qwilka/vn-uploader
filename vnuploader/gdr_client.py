
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


def initialize(conf=None):
    app_config = vn_config.initialize(conf)
    try:
        get_girderclient()
    except Exception as err:
        logger.error("initialize: failed with error «%s»." % (err,) )
        return False
    return app_config


def get_girderclient():
    global cache_gc
    if cache_gc:
        print("*** cache_gc ***")
        return cache_gc
    else:
        _config = vn_config.app_config
        gc = girder_client.GirderClient(apiUrl=_config["girder"]["apiUrl"])
        gc.authenticate(apiKey=_config["girder"]["apiKey"])
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



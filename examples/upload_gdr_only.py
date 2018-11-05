import logging
import sys

from vnuploader.dataset import UploadDataset
from vnuploader import gdr_client
from vnuploader import uri
from vnuploader.vn_config import load_config

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
lh = logging.StreamHandler()
logger.addHandler(lh)

conf = {
    "database": {
        "host": "localhost",
        "port": 3005,
        "db": "testing"
    },
    "girder": {
        "api_url": "http://localhost:8080/api/v1",
        "api_key": "EdKeaqELS40XIrepHcXZFuLQzrMOGUJOVIeeyR5Z",
    },
}

fs_path = "/home/develop/testdata/L51_dataset_testing"

retVal = gdr_client.initialize(conf=conf)
if retVal:
    logger.info("%s: initialize with config=«%s»" % (__file__,retVal))
else:
    logger.error("%s: failed to initialize." % (__file__,))
    sys.exit(1)

collection = gdr_client.get_collection(
                name="Skarv-subsea-pipelines_IMS_2015",
                description="Skarv subsea pipelines, 2015 integrity survey data."
            )

ds_meta = {
    "description": "testing data upload",
    "gdr": {
        "collection_name": collection["name"],
        "collection_desc": collection["description"],
        "collection_id": collection["_id"],
    },
    "state": {
        "country": "NOR",
        "field": "SKARV",
        "domain": "SUBSEA",
        "survey": "IMS",
        "date": "2015",
        "statespace": "DS",        
    },
}

ds_name = uri.state2string(ds_meta["state"], "${country}-${field}-${domain}_${survey}_${date}")
ds = UploadDataset(ds_name, fs_path, collection["_id"], ds_meta)
ds.rootnode = ds.make_fs_tree()
ds.set_dstree(ds.rootnode, desc="Original file system tree.")
ds.rootnode = ds.fs_tree_extend()
ds.set_dstree(ds.rootnode, treename="zfs", desc="Extended fs, including zipped files.")
ds.gdr_upload()

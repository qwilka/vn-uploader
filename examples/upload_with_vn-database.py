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
        "apiUrl": "http://localhost:8080/api/v1",
        "apiKey": "EdKeaqELS40XIrepHcXZFuLQzrMOGUJOVIeeyR5Z",
    },
}

fs_path = "/home/develop/testdata/L51_dataset_testing"

UPLOAD_GIRDER = True
UPLOAD_VISINUM = True   # True: upload additional metadata Visinum
CHECK = False

retVal = gdr_client.initialize(conf=conf)
if retVal:
    logger.info("%s: initialize with config=«%s»" % (__file__,retVal))
else:
    logger.error("%s: failed to initialize." % (__file__,))
    sys.exit(1)

gdr_coll_name = "Skarv-subsea-pipelines_IMS_2015"
# collection = gdr_client.get_collection(
#                 name=gdr_coll_name,
#                 description="Skarv subsea pipelines, 2015 integrity survey data."
#             )

        # "gdr": {
        #     "collection_name": collection["name"],
        #     "collection_desc": collection["description"],
        #     "collection_id": collection["_id"],
        # },


if UPLOAD_GIRDER:
    ds_meta = {
        "desc": "testing data upload",
        "state": {
            "country": "NOR",
            "field": "SKARV",
            "asset": "SUBSEA",
            "activity": "IMS",
            "date": "2015",
            "statespace": "DS",        
        },
    }

    ds_name = uri.state2string(ds_meta["state"], "${country}-${field}-${asset}_${activity}_${date}")
    ds = UploadDataset(ds_name, fs_path, gdr_coll_name, ds_meta)
    ds.dstree = ds.make_fs_tree()
    ds.set_dstree(ds.dstree, desc="Original file system tree.")
    ds.gdr_upload()

if UPLOAD_VISINUM:
    ds.dstree = ds.fs_tree_extend()
    ds.set_dstree(ds.dstree, treename="zfs", desc="Extended fs, including zipped files.")
    ds.db_insert()
    ds.dstree.db_insert(recursive=True)

# if CHECK:
#     parentFolder = "5bf45ecd290a5440bcc75fa9"
#     dstree = gdr_client.folder_to_tree(parentFolder)
#     print(dstree)

import logging
import sys

logger = logging.getLogger(__name__)

from vnuploader.dataset import UploadDataset
from vnuploader import gdr_client
from vnuploader import uri
from vnuploader.vn_config import load_config

# conf = """
# [girder]
# apiUrl = "http://localhost:8080/api/v1"
# apiKey = "EdKeaqELS40XIrepHcXZFuLQzrMOGUJOVIeeyR5Z"
# assetstore_id = "5bae4900290a5428c1affdf0"

# [database]
# host = "localhost"
# port = 3005
# db = "testing"
# """

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

checkconf = load_config(conf)
print("local_config=", checkconf)

fs_path = "/home/develop/testdata/L51_dataset_testing"

VN_DATABASE = True

retVal = gdr_client.initiailize()
if not retVal:
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
#ds_meta[""]
print("ds_name", ds_name)
print(uri.state2uuid(ds_meta["state"]))
ds = UploadDataset(ds_name, fs_path, collection["_id"], ds_meta)
ds.rootnode = ds.make_fs_tree()
ds.set_dstree(ds.rootnode, desc="Original file system tree.")
ds.gdr_upload()

if VN_DATABASE:
    ds.rootnode = ds.fs_tree_extend()
    ds.set_dstree(ds.rootnode, treename="zfs", desc="Extended fs, including zipped files.")
    ds.db_insert()
    ds.rootnode.db_insert(recursive=True)

# try:
#     ds = UploadDataset(ds_name, fs_path, collection["_id"], ds_meta)
# except Exception as err:
#     logger.error('%s: cannot create UploadDataset; «%s»' % (__file__, err))
# upload_fs_dataset(fs_path, collection_id,  
#                     meta=None)
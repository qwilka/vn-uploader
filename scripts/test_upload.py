# import datetime

# import requests

from vnuploader.dataset import UploadDataset
from vnuploader import gdr_client
#from gdr_client import upload_fs_dataset, delete_folder
from vnuploader.vn_utilities import make_survey_uri


fs_path = "/home/develop/testdata/L51_dataset_testing"
#collection_id = "5b6479da20e05d37680264f4"

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
    "gdr": {
        "collection_name": "NOR::SKARV::SUBSEA",
        "collection_desc": "Skarv subsea data.",
        "collection_id": None,
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

#retVal = upload_fs_dataset(fs_path, collection_id, meta=meta, clean=True)

# folder_id = "5b71d25420e05d214a33a1e6"
# retVal =  delete_folder(folder_id)
# print(retVal)

UPLOAD = False

coll_uri, _ = make_survey_uri(ast_uri=ast_uri, svy_tag=svy_tag, vn_date=vn_date)
collection = gdr_client.get_collection(coll_uri, desc="Skarv 2015 pipeline IMS survey data.")
print(collection)
ds_meta["gdr"] = {
    "collection" : {
        "_id": collection["_id"],
        "name": collection["name"],
    },
}
ds_name = ds_meta["name"]
vn_uri, _ = make_survey_uri(ast_uri=ast_uri, svy_tag=svy_tag, vn_date=vn_date, vn_cat="ds", location=ds_name)
ds = UploadDataset(fs_path, coll_uri, vn_uri, ds_name, ds_meta)
# ds.rootnode = ds.make_fs_tree()
# ds.set_dstree(ds.rootnode, desc="Original file system tree.")
# ds.rootnode = ds.fs_tree_extend()
# ds.set_dstree(ds.rootnode, treename="vfs", desc="Extended fs, including zipped files.")
# ds.db_insert()
# retVal = ds.gdr_upload()
# ds.rootnode.db_insert(recursive=True)
# print(retVal)

# if UPLOAD:
#     vn_uri, _ = make_survey_uri(ast_uri=ast_uri, svy_tag=svy_tag, vn_date=vn_date)
#     #print(vn_uri)
#     ds = UploadDataset(fs_path, vn_uri, "test dataset", ds_meta)
#     print(ds.rootnode.to_texttree())
#     ds.rootnode.db_insert(recursive=True)
#     retVal = ds.gdr_upload()
#     print(retVal)

# headers = {
#     "Content-Type": "application/x-www-form-urlencoded",
#     "Accept": "application/json",
#     "Girder-Token": "0JZQtT85S2TRmgWOGuocLwkNHptUTWUXfB5HqEhE1HGh4DzQX7h5xUbvuqedarak",
# }
# datenow = datetime.datetime.now(datetime.timezone.utc)
# dbdoc = {
#     "_id": "testid2", 
#     'key1': 'value2',
#     "timestamp": datetime.datetime.now(datetime.timezone.utc),
# }
# import json
# #{"_id": "testid2", 'key1': 'value1', 'date': datetime.datetime.now(datetime.timezone.utc)}
# rdata = {
#     "doc": json.dumps(dbdoc, default=str),
# }
# # import json
# # rdata = json.dumps(rdata)
# # r = requests.post("http://localhost:8080/api/v1/visinum/vn_create_item", 
# #     headers=headers,
# #     params={"collection":"dataset"}, 
# #     data=rdata)
# params = {
#     "collection": "dataset",
#     "vn_datetime": True,
# }
# r = requests.post("http://localhost:8080/api/v1/visinum/vn_create_item", 
#     params=params, data=rdata)
# print(r.url)
# print(r.headers)
# print(r.text)

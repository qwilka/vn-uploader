import datetime

from dataset import UploadDataset
from gdr_client import upload_fs_dataset, delete_folder
from utilities import make_survey_uri


fs_path = "/home/develop/testdata/L51_dataset_testing"
collection_id = "5b6479da20e05d37680264f4"

ast_uri = "NOR::SKARV::SUBSEA"
svy_tag = "svy_tag"
vn_date = "2015"

ds_meta = {
    "name": "SPL-IMS-2015_MBES_FVP",
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


vn_uri, _ = make_survey_uri(ast_uri=ast_uri, svy_tag=svy_tag, vn_date=vn_date)
#print(vn_uri)
ds = UploadDataset(fs_path, "test-dataset-Wupload", vn_uri, ds_meta)
print(ds.rootnode.to_texttree())


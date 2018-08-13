import datetime

from gdr_client import upload_fs_dataset, delete_folder


fs_path = "/home/develop/Downloads/data/L51_dataset_testing"
collection_id = "5b6479da20e05d37680264f4"
meta = {
    "met1": 1234,
    "ns": {
        "met2": "two",
        "ts": datetime.datetime.now(datetime.timezone.utc),
    }
}

retVal = upload_fs_dataset(fs_path, collection_id, meta=meta, clean=True)

# folder_id = "5b71d25420e05d214a33a1e6"
# retVal =  delete_folder(folder_id)

print(retVal)
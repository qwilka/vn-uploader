from vnuploader.dataset import UploadDataset
from vnuploader import gdr_client
from  vnuploader import vn_uri
from vnuploader.vn_config import load_config

conf = """
[girder]
apiUrl = "http://localhost:8080/api/v1"
apiKey = "EdKeaqELS40XIrepHcXZFuLQzrMOGUJOVIeeyR5Z"
assetstore_id = "5bae4900290a5428c1affdf0"

[database]
host = "localhost"
port = 3005
db = "testing"
"""

checkconf = load_config(conf)
print("local_config=", checkconf)



collection = gdr_client.get_collection(
                name="Skarv-subsea-pipelines_IMS_2015",
                description="Skarv subsea pipelines, 2015 integrity survey data."
            )

ds_state = {
    "country": "NOR",
    "field": "SKARV",
    "domain": "SUBSEA",
    "survey": "IMS",
    "date": "2015",
    "statespace": "DS",
}

print(vn_uri.state2string(ds_state, "${country}-${field}-${domain}_${survey}_${date}"))
print(vn_uri.state2uuid(ds_state))


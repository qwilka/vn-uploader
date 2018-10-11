import copy
import logging
import os

import toml

# from ..vn_girder import gr_users
# from . import vn_URI
from . import vn_utilities

logger = logging.getLogger(__name__)


default_conf = """
[girder]
apiKey = "EdKeaqELS40XIrepHcXZFuLQzrMOGUJOVIeeyR5Z"
apiUrl = "http://localhost:8080/api/v1"

[database]
host = "localhost"
port = 3004
db = "visinum"
"""


def load_config(filepath):
    global app_config
    _config = {}
    if isinstance(filepath, str) and os.path.isfile(filepath):
        try:
            with open(filepath, 'r') as conf_fh:
                _config = toml.load(conf_fh)
        except Exception as err: 
            logger.error("load_config: cannot load config file «%s», failed with error «%s»." % (filepath, err) )
    elif isinstance(filepath, str):
        try:
            _config = toml.loads(filepath)
        except Exception as err: 
            logger.error("load_config: cannot load config string «%s», failed with error «%s»." % (filepath, err) )
    else:
        logger.error("load_config: cannot find config file «%s» in directory «%s»" % (filepath, os.getcwd()) )
    vn_utilities.rupdate(app_config, _config)
    return app_config


if "app_config" not in globals():  # initiailize (this is not necessary???)
    app_config = {}
    load_config(default_conf)
print("vn-config globals():", globals()["app_config"])


def get_config(*keys):
    global app_config
    if not keys:
        return False
    _datadict = copy.deepcopy(app_config)
    for _key in keys:
        _val = _datadict.get(_key, None)
        if isinstance(_val, dict):
            _datadict = _val
        else:
            break
    return _val


# def get_db_uri(db=None, collection=None, _id=None, vn_uri=None, **kwargs):
#     _db_uri = get_config("database")
#     _db_uri.update(kwargs)
#     if db:
#         _db_uri["db"] = db
#     if collection:
#         _db_uri["collection"] = collection
#     if not _id and vn_uri:
#         _id = vn_URI.string2UUID(vn_uri)
#     if _id:
#         _db_uri["_id"] = _id
#     return _db_uri


# def get_girder_dct():
#     _girder_dct = get_config("girder")
#     _api_key = _girder_dct["api_key"]
#     _girder_dct["admin"] = gr_users.get_user_from_API_key(_api_key, False)
#     return _girder_dct


def make_req_url(*paths):
    apiUrl = app_config["girder"]["apiUrl"] 
    if apiUrl.endswith("/"):
        apiUrl = apiUrl[:-1]
    return apiUrl + "/" + "/".join(paths)

import collections
import json
import logging
import operator

logger = logging.getLogger(__name__)

import requests
from vntree import Node, NodeAttr

from . import vn_config


class UploadNode(Node):
    _id = NodeAttr()
    vn_uri = NodeAttr("vn")
    db_uri = NodeAttr("vn")

    def __init__(self, name=None, parent=None, data=None, treedict=None):
        super().__init__(name, parent, data, treedict)
        self.data = collections.defaultdict(dict, self.data)

    def to_treedict(self, recursive=True, full=True):
        _dct = collections.defaultdict(dict)
        if full:
            _dct = {k:v for k, v in vars(self).items() if k not in ["parent", "childs"]}
        else:
            _dct["data"]["vn"] = self.get_data("vn")
            _dct["name"] = self.name
            _dct["_id"] = self._id
        if recursive and self.childs:
            _dct["childs"] = []
            for _child in self.childs:
                _dct["childs"].append( _child.to_treedict(recursive=recursive, full=full) )
        return _dct 

    def db_insert(self, recursive=False):
        retval = None
        _config = vn_config.app_config
        if recursive:
            retval = list(map(operator.methodcaller('db_insert', recursive=False), self))
            retval = retval[0]
        else:
            _doc = self.get_data()
            if self.childs:
                _doc["childs"] = []
                for _child in self.childs:
                    _doc["childs"].append(_child._id)
            _doc["parent"] = self.parent and self.parent._id
            rdata = {
                "doc": json.dumps(_doc, default=str),
            }
            params = {
                "db": _config["database"]["db"],
                "collection": self.db_uri["collection"],
                "vn_datetime": True,
            }
            #req = requests.post("http://localhost:8080/api/v1/visinum/vn_create_item", 
            req_url = vn_config.make_req_url("visinum/vn_create_item")
            req = requests.post(req_url, params=params, data=rdata)
            if req.status_code != 200:
                logger.debug('%s.db_insert response %s' % (self.__class__.__name__, req.text))
            retval = req.text
        return retval 



import datetime
import json
import logging
import os
import platform
import re
import sys
import uuid




logger = logging.getLogger(__name__)





def string2UUID(namestring):
    """Calculate a version3 UUID from a string. 
    
    :param namestring: string, string used to calculate v.3 UUID
    :returns: string, UUID in hex format
    
    Examples:
    >>> string2UUID('/home/develop')
    '53cd6254b2d0377d8143c8714c1134c1'
    """
    return uuid.uuid3(uuid.NAMESPACE_URL, namestring).hex


def make_fs_uri(fs_path=None, path2=None, fs_dev_uuid=None, uri=None):
    _sep1 = "||"
    if uri:
        uri_li = uri.split(_sep1)
        if not fs_dev_uuid:
            fs_dev_uuid = uri_li[0]
        if not fs_path and len(uri_li)>=2:
            fs_path = uri_li[1]
        if not path2 and len(uri_li)>=3:
            path2 = uri_li[2]
    if fs_dev_uuid:
        _URI = fs_dev_uuid
    else:
        _URI = ""
    _URI += _sep1 + fs_path 
    if path2:
        _URI += _sep1 + path2
    _hash = string2UUID(_URI)
    return _URI, _hash


def make_survey_uri(ast_uri="", svy_tag="", vn_date="", vn_cat="", vn_uri=None, location=None):
    _sep1 = "||"
    if vn_uri:
        uri_li = vn_uri.split(_sep1)
        if not ast_uri:
            ast_uri = uri_li[0]
        if not svy_tag and len(uri_li)>=2:
            svy_tag = uri_li[1]
        if not vn_date and len(uri_li)>=3:
            vn_date = uri_li[2]
        if not vn_cat and len(uri_li)>=4:
            vn_cat = uri_li[3]
    _URI = ""
    _URI +=  ast_uri.upper() 
    _URI +=  _sep1 + svy_tag.upper()
    _URI +=  _sep1 + vn_date.upper()
    _URI +=  _sep1 + vn_cat.upper()
    if location:
        _URI +=  _sep1 + location.upper()
    _hash = string2UUID(_URI)
    return _URI, _hash



def obj2dict(obj):
    # http://stackoverflow.com/a/61522
    _dict={}
    _dirList = dir(obj)
    for key in _dirList:
        if key.startswith("__"): continue
        val = getattr(obj, key)
        if callable(val): continue
        if isinstance(val, bytes):
            _dict[key] = val.decode()
        else:
            _dict[key] = val
    return _dict

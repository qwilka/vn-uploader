import collections
import datetime
import logging
import mimetypes
import os
import pathlib
import platform
import subprocess

from system import dirDev2UUID, make_vn_fs_URI
import vn_tree


logger = logging.getLogger(__name__)


try:
    import magic
    magic_imported = True
except:
    magic_imported = False  


def basic_fs_metadata(fs_path, rel_rootdir=None):
    meta = {}
    _ppath = pathlib.Path(fs_path)
    meta["fs_path"] = _ppath.as_posix()
    meta["fs_name"] = _ppath.name
    meta["fs_ext"] = _ppath.suffix
    if rel_rootdir:
        root_pp = pathlib.Path(rel_rootdir)
        if root_pp.parent.is_dir():
            meta["fs_relpath"] = str(_ppath.relative_to(root_pp.parent))
    if _ppath.is_dir():
        meta["fs_cat"] = "folder"
        meta["fs_mime"] = "inode/directory"
    if _ppath.is_file():
        meta["fs_cat"] = "file"
        if platform.system() in ['Linux', 'Darwin']:
            meta["fs_filecmd"] = file_command_metadata(fs_path)
            meta["fs_mime"] = meta["fs_filecmd"].get("mime", None)
        if magic_imported:
            meta["fs_magic"] = file_magic(fs_path)
            _fmime = meta.get("mime", None)
            if not _fmime:
                meta["fs_mime"] = meta["fs_magic"].get("mime", None)
    meta["fs_stat"] = obj2dict(_ppath.stat()) 
    try:
        meta["permissions"] = {
            "owner": _ppath.owner(),
            "group": _ppath.group(),
            "umask": str(oct(meta["fs_stat"]["st_mode"])),
        }
    except KeyError:
        pass
    return meta



def file_magic(fs_path):
    if not magic_imported:
        return None
    _meta = {}
    _meta["type"] = magic.from_file(fs_path)
    _meta["mime"] = magic.from_file(fs_path, mime=True)    
    return _meta


def file_command_metadata(fpath): 
    """file command metadata"""
    if not platform.system() in ['Linux', 'Darwin']:
        return False
    mdata={}
    sys_cmds = [ ("type", ['file', '--brief', fpath] ), 
     ("mime", ['file', '--mime-type', '-b', fpath] ),
     ("encoding", ['file', '--mime-encoding', '-b', fpath] ) ]
    for k, cmdli in sys_cmds:
        try:
            # running subprocess.check_output to check for exception, however this returns bytes
            op = subprocess.check_output(cmdli)
        except subprocess.CalledProcessError as err:
            logger.warning("WARN fs_meta.file_command_metadata %s: %s" % (fpath, err) )
        else:
            # avoid running system call twice, so must convert bytes
            mdata[k] = op.decode(encoding='UTF-8').strip()
    return mdata


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


def make_fs_tree(fs_path, parent=None, meta=None):
    fs_pp = pathlib.Path(fs_path)
    fs_dev_uuid = dirDev2UUID(fs_path)
    if not fs_pp.exists():
        logger.error("make_fs_tree: arg «fs_path» does not exist: %s" % (fs_path,))
        return False
    _meta = collections.defaultdict(dict)
    if meta and isinstance(meta, dict):
        _meta.update(meta)
    _meta["fs"] = basic_fs_metadata(fs_path, fs_path)
    rootnode = vn_tree.VnNode(fs_pp.name, parent, data=_meta )
    for localdir, dirs, files in os.walk(fs_path, topdown=True):
        _loc_pp = pathlib.Path(localdir)
        _loc_relpp = _loc_pp.relative_to(fs_path)
        _parent = rootnode.get_nodepath(_loc_relpp.as_posix())
        if _parent:
            for dirname in dirs:
                _dir_pp =  _loc_pp / dirname 
                _meta["fs"] = basic_fs_metadata(_dir_pp.as_posix(), fs_path)
                vn_tree.VnNode(_dir_pp.name, _parent, _meta )
            for filename in files:
                _file_pp =  _loc_pp / filename
                _meta["fs"] = basic_fs_metadata(_file_pp.as_posix(), fs_path)
                vn_tree.VnNode(_file_pp.name, _parent, _meta )
    for _node in rootnode:
        #_node_fs_path = _node.get_meta("fs_path")
        _node_fs_path = _node.get_data("fs", "fs_path")
        _pp = pathlib.Path(_node_fs_path)
        _relpath = _pp.relative_to(fs_path).as_posix()
        _uri, _hash = make_vn_fs_URI(_node_fs_path, 
                        fs_dev_uuid=fs_dev_uuid)
        _node.set_data("vn", "vn_uri", value=_uri)
        _node.set_data("fs", "fs_dev_uuid", value=fs_dev_uuid)
        _node.set_data("fs", "fs_uri", value=_uri)
        #_node.set_data("vn", "vn_uri_hash", value=_hash)
        _db_uri = {
            "db": "visinum",
            "collection": "fs",
            "_id": _hash,                
        }
        _node._id = _hash
        _node.set_data("vn", "db_uri", value=_db_uri)
        _node.set_data("vn", "vn_root_id", value=rootnode._id)
        _node.set_data("vn", "vn_timestamp", value=datetime.datetime.now(datetime.timezone.utc)) 
        #_node.set_data("vn", "fs_cat", value=_node.get_data("fs", "fs_cat")) # "vn_filetype"
        #_node.set_data("vn", "ast_uri", value=None)
        #_node.set_data("vn", "vn_date", value=self.get_data("vn", "vn_date"))
        #_node.set_data("vn", "vn_cat", value="fs")
        #_node.set_data("vn", "vn_filetype", value=_vn_filetype)
        #_node.set_data("vn", "vn_transform", value=dict())
    return rootnode







if __name__=="__main__":
    fs_path = "/home/develop/Downloads/data/L51_dataset_testing"
    rootnode = make_fs_tree(fs_path)
    print(rootnode.to_texttree())
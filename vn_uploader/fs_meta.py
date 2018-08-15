import collections
import datetime
import getpass
import logging
import mimetypes
import os
import pathlib
import platform
import stat
import subprocess

import utilities
from vn_tree import UploadNode


logger = logging.getLogger(__name__)


try:
    import magic
    magic_imported = True
except:
    magic_imported = False  

try:
    import pyudev
    pyudev_imported = True
except:
    pyudev_imported = False


def platform_metadata(fs_path):
    _fs_dev_uuid = dirDev2UUID(fs_path)
    meta = {
            "platform": platform_info(),
            "fs_dev_uuid": _fs_dev_uuid,
            "user": user_info(),
            "vn_timestamp": datetime.datetime.now(datetime.timezone.utc),
    }
    return meta



def platform_info():
    meta = {"system": platform.system(),
            "node": platform.node(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python_version": platform.python_version(),
            "cwd": os.getcwd()
            }
    return meta


def user_info():
    metaObj = {"name": getpass.getuser()
             }
    return metaObj


def dirDev2UUID(dirpath):
    # https://stackoverflow.com/questions/7551546/getting-friendly-device-names-in-python
    # /sbin/udevadm info --export-db 
    fs_dev_UUID = None
    if platform.system() == 'Linux':
        if pyudev_imported:
            udevcontext = pyudev.Context()
            device_num = os.stat(dirpath)[stat.ST_DEV]
            try:
                device = pyudev.Device.from_device_number(udevcontext, 'block', device_num)
            except pyudev.DeviceNotFoundByNumberError as err:
                logger.debug("dirDev2UUID: pyudev.DeviceNotFoundByNumberError for dirpath %s; %s" % (dirpath, err))
                # if dirpath in user encrypted directory, cannot get device UUID
                # try again, with top-level directory...
                #_dpath = os.path.splitdrive(dirpath)[1]
                _dparts = dirpath.split(os.path.sep)
                if _dparts[0] == "":
                    _dparts.pop(0)
                _dpath = os.path.sep + _dparts[0]
                # if len(_dparts)>1: 
                #     _dpath = _dpath + os.path.sep + _dparts[1]
                device_num = os.stat(_dpath)[stat.ST_DEV]
                try:
                    device = pyudev.Device.from_device_number(udevcontext, 'block', device_num)
                except pyudev.DeviceNotFoundByNumberError:
                    device = {} # give up...
                # TODO: better way?
                # du -h dirpath # get device name and insert in place of 'sda5' below
                # device = pyudev.Device.from_name(context, 'block', 'sda5')
            if 'ID_FS_UUID' in device.keys():
                fs_dev_UUID = device['ID_FS_UUID']
        else:
            logger.warning("WARN system.dirDev2UUID requires module pyudev; %s" % (dirpath,))
    elif platform.system() == 'Windows':
        # on Windows use command mountvol 
        sysret = subprocess.check_output(['mountvol', os.path.splitdrive(os.path.abspath(dirpath))[0], '/L'])
        op = sysret.decode(encoding='UTF-8').strip()
        regex = re.compile("Volume{([\\w|-]+)}") 
        hit = regex.search(op) 
        if hit:
            fs_dev_UUID = hit.groups()[0]
        else:
            logger.error("ERROR system.dirDev2UUID cannot identify device for %s" % (dirpath,))
    elif platform.system() == 'Darwin':
        logger.error("ERROR system.dirDev2UUID not implemeted on OS X; %s" % (dirpath,))
    return fs_dev_UUID


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
    meta["fs_stat"] = utilities.obj2dict(_ppath.stat()) 
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
    rootnode = UploadNode(fs_pp.name, parent, data=_meta )
    for localdir, dirs, files in os.walk(fs_path, topdown=True):
        _loc_pp = pathlib.Path(localdir)
        _loc_relpp = _loc_pp.relative_to(fs_path)
        _parent = rootnode.get_nodepath(_loc_relpp.as_posix())
        if _parent:
            for dirname in dirs:
                _dir_pp =  _loc_pp / dirname 
                _meta["fs"] = basic_fs_metadata(_dir_pp.as_posix(), fs_path)
                UploadNode(_dir_pp.name, _parent, _meta )
            for filename in files:
                _file_pp =  _loc_pp / filename
                _meta["fs"] = basic_fs_metadata(_file_pp.as_posix(), fs_path)
                UploadNode(_file_pp.name, _parent, _meta )
    for _node in rootnode:
        #_node_fs_path = _node.get_meta("fs_path")
        _node_fs_path = _node.get_data("fs", "fs_path")
        _pp = pathlib.Path(_node_fs_path)
        _relpath = _pp.relative_to(fs_path).as_posix()
        _uri, _hash = utilities.make_fs_uri(_node_fs_path, 
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
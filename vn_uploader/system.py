
import getpass
import json
import logging
import os
import platform
import re
import stat
import subprocess
import sys
import uuid

try:
    import pyudev
    pyudev_imported = True
except:
    pyudev_imported = False


logger = logging.getLogger(__name__)




def platform_info():
    metaObj = {"system": platform.system(),
            "node": platform.node(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python_version": platform.python_version(),
            "cwd": os.getcwd()
            }
    return metaObj


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


def string2UUID(namestring):
    """Calculate a version3 UUID from a string. 
    
    :param namestring: string, string used to calculate v.3 UUID
    :returns: string, UUID in hex format
    
    Examples:
    >>> string2UUID('/home/develop')
    '53cd6254b2d0377d8143c8714c1134c1'
    """
    return uuid.uuid3(uuid.NAMESPACE_URL, namestring).hex


def make_vn_fs_URI(fs_path=None, path2=None, fs_dev_uuid=None, uri=None):
    #_fs_path2 = None
    # if isinstance(fs_path, str):
    #     _fs_path1 = fs_path
    # elif isinstance(fs_path, (list, tuple)):
    #     _fs_path1 = fs_path[0]
    #     if len(fs_path)>1:
    #         _fs_path2 = fs_path[1]
    # else:
    #     logger.error(" make_vn_fs_URI: argument fs_path not correct %s" % (str(fs_path),))
    #     return False
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


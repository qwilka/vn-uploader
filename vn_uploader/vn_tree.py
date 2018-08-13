
import collections
import copy
import itertools
import logging
import operator
import pathlib 

logger = logging.getLogger(__name__)

class Node:
    """Node class for tree data structure.  
    """
    def __init__(self, name=None, parent=None, data=None, treedict=None):
        if data and isinstance(data, dict):
            self.data = copy.deepcopy(data)
        else:
            self.data = {}
        if name and isinstance(name, str):
            self.name = name
        elif "name" in self.data:
            self.name = self.data["name"]
        else:
            self.name = ""
        self.childs = []
        if parent and isinstance(parent, Node):
            parent.add_child(self)
        elif parent is None:
            self.parent = parent
        else:
            raise TypeError("Node instance «{}» argument «parent» type not valid: {}".format(name, type(parent)))
        if treedict and isinstance(treedict, dict):
            self.from_treedict(treedict, parent)
        if not self.name:
            raise ValueError("{}: «name» argument not correctly specified; {}".format(self.__class__.__name__, self.data))

    def __iter__(self): 
        yield self  
        for node in itertools.chain(*map(iter, self.childs)):
            yield node

    def __reversed__(self):  
        for node in itertools.chain(*map(reversed, self.childs)):
            yield node
        yield self 

    def add_child(self, newnode):
        self.childs.append(newnode)
        newnode.parent = self
        return True    

    def remove_child(self, node):
        self.childs.remove(node)
        return True

    @property
    def path(self):
        # NOTE returns node path in form "root/child/grandchild"
        _path = pathlib.PurePosixPath(self.name)
        _node = self
        while _node.parent:
            _path = _node.parent.name / _path
            _node = _node.parent
        return _path.as_posix()

    def get_data(self, *keys):
        if not keys:
            #return copy.deepcopy(self.data)
            _val = self.data
        _datadict = self.data
        for _key in keys:
            _val = _datadict.get(_key, None)
            if isinstance(_val, dict):
                _datadict = _val
            else:
                break
        if isinstance(_val, dict):
            _val = copy.deepcopy(_val)
        return _val

    def set_data(self, *keys, value):
        # Note that «value» is a keyword-only argument
        _datadict = self.data
        for ii, _key in enumerate(keys):
            if ii==len(keys)-1:
                _datadict[_key] = value
            else:
                if _key not in _datadict:
                    _datadict[_key] = {}
                _datadict = _datadict[_key]
        return True

    def get_ancestors(self):
        # return list of ancestor nodes starting with self.parent and ending with root
        ancestors=[]
        _curnode = self
        while _curnode.parent:
            _curnode = _curnode.parent
            ancestors.append(_curnode)
        return ancestors

    def get_child_by_name(self, childname):
        _childs = [_child for _child in self.childs if _child.name==childname]
        if len(_childs)>1:
            logger.warning("%s.get_child_by_name: node:«%s» has more than 1 childnode with name=«%s»." % (self.__class__.__name__, self.name, childname))
        if len(_childs)==0:
            _childnode = None
        else:
            _childnode = _childs[0] # return first node found with name childname
        return _childnode

    def get_nodepath(self, path=None):
        # get decendent node from path (NOTE: path is relative to self)
        _node = self
        if path is None or path=="." or path=="./" or path=="":
            pass
        else:
            _pathlist = path.split(pathlib.posixpath.sep) 
            for _nodename in _pathlist:
                if _nodename in [self.name, ".", ""]:
                    continue
                try:
                    _node = _node.get_child_by_name(_nodename)
                except:
                    _node = None
                    break
        return _node

    def find_one_node(self, *keys, value):
        # Note that «value» is a keyword-only argument
        for _node in self:
            _val = _node.get_data(*keys)
            if _val == value:
                return _node
        return None

    def to_texttree(self):
        treetext = ""
        local_root_level = len(self.get_ancestors())
        for node in self: 
            level = len(node.get_ancestors()) - local_root_level
            treetext += ("." + " "*3)*level + "|---{}\n".format(node.name)
        return treetext

    def from_treedict(self, treedict, parent=None):
        if "data" in treedict:
            self.data = collections.defaultdict(dict, treedict["data"])
        #self.name = "name not set"  # default values for name and data, should be over-written
        #self.data = {}
        for key, val in treedict.items():
            if key in ["parent", "childs", "data"]:
                continue
            setattr(self, key, val)
        # if parent:
        #     parent.add_child(self)
        if "childs" in treedict.keys():
            for _childdict in treedict["childs"]:
                #self.childs.append( self.__class__(parent=self, treedict=_childdict) )
                self.__class__(parent=self, treedict=_childdict)

    def to_treedict(self, recursive=True):
        # NOTE: replace vars(self) with self.__dict__ ( and self.__class__.__dict__ ?)
        _dct = {k:v for k, v in vars(self).items() if k not in ["parent", "childs"]}
        if recursive and self.childs:
            _dct["childs"] = []
            for _child in self.childs:
                _dct["childs"].append( _child.to_treedict(recursive=recursive) )
        return _dct 




VnNode = Node









if __name__ == "__main__":
    # python -m visinum.meta.vn_tree2
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    logger.addHandler(ch)

    SIMPLE_TREE = True

    if SIMPLE_TREE:

        rootnode   = Node('ROOT (level 0, the "top" of the tree)')
        Node("1st child (level 1, leaf node)", parent=rootnode)
        child2 = Node("2nd child (level 1)", rootnode)
        Node("grand-child1 (level 2, leaf node)", child2)
        Node("grand-child2 (level 2, leaf node)", child2)
        child3 = Node("3rd child (level 1)", rootnode)
        Node("another child (level 1, leaf node)", rootnode)
        grandchild3 = Node(parent=child3, name="grand-child3 (level 2")
        ggrandchild = Node("great-grandchild (level 3)", grandchild3)
        Node("great-great-grandchild (level4, leaf node)", ggrandchild)
        Node("great-grandchild2 (level 3, leaf node)", grandchild3)

        print(rootnode.to_texttree())
        # for ii, node in enumerate(rootnode):
        #     print("{} top-down «{}»".format(ii, node.name))
        # for ii, node in enumerate(reversed(rootnode)):
        #     print("{} bottom-up «{}»".format(ii, node.name))


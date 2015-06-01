#!/usr/bin/python

"""
(C) Copyright 2015 Jesse Hogan <jessehogan0@gmail.com>
This file is part of PyBusiness.

PyBusiness is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

PyBusiness is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with PyBusiness.  If not, see <http://www.gnu.org/licenses/>.
"""

import pdb;
import collections
b=pdb.set_trace

def gettergetter(method):
    def getter(obj):
        return getattr(obj, method)()
    return getter


class col(object):
    def __init__(self, items=None):
        self.clear()
        if items != None:
            self.add(items)

    def clear(self):
        self.list=[]

    def filter(self, f):
        r=self.__class__()
        l=filter(f, self)
        r.list=l
        return r

    def reverse(self):
        self.list.reverse()

    def isempty(self):
        return self.len() == 0

    def __iter__(self):
        for e in self.list:
            yield e

    def sort(self, orderby):
        orderbys = orderby.split(',')
        new=[]
        orderbys.reverse()
        for orderby in orderbys:

            orderby=orderby.split()

            sortorder=None
            if len(orderby) == 2:
                sortorder = orderby[1].lower()

            orderby=orderby[0]

            reverse=sortorder == 'desc'

            f=gettergetter(orderby)

            self.list.sort(
                key=gettergetter(orderby), 
                reverse=reverse)

    def __setitem__(self, key, item):
        self.list[key]=item

    def add(self, e, justadd=False):
        """ justadd means: don't worry about 
        whether or not e is a collection, just
        add it. """
        if not justadd and \
            (isinstance(e, col) or \
            isinstance(e, collections.Iterable)):
            es=e
            for e in es:
                self.list.append(e)
            e=es
        else:
            self.list.append(e)
        return e

    def adduniq(self, e, eqtype='obj'):
        if not self.contains(e, eqtype):
            return self.add(e)
        return None

    def len(self):
        return len(self.list)

    def eq(self, testcol):
        return self.list == testcol.list

    def __len__(self):
        # Not tested
        return self.len()

    def __isub__(self, r):
        self.remove(r)
        return self

    def unshift(self, o):
        self.list.insert(0, o)

    def remove(self, obj):
        if isinstance(obj, int): obj=self[obj]
        if obj!=None: self.list.remove(obj)
        return obj
        
    def __iadd__(self, r):
        self.add(r)
        return self

    def __add__(self, other):
        r = other.__class__()
        for o in self: r.add(o)
        for o in other: r.add(o)
        return r

    def __getitem__(self, key):
        if key.__class__ == int:
            return self.list[key]

        keyisobj = (type(key) != str and
                    type(key) != unicode)
        for e in self.list:
            if keyisobj:
                if e is key:
                    return e
            else:
                if e.str() == key:
                    return e

    def get(self, ix):
        return self[ix]
    
    def set(self, ix, item):
        self.list[ix]=item

    def contains(self, obj, eqtest='obj'):
        if eqtest == 'obj':
            for obj0 in self:
                if obj is obj0:
                    return True
        elif eqtest == 'id':
            return self.byid(obj.id()) != None
        else:
            msg="Invaild eqtest: "+eqtest
            raise Exception(msg)
        return False

    def byid(self, id):
        for e in self:
            if e.id() == id: return e
        return None


    def distinct(self, prop):
        r=[]
        for e in self:
            e=getattr(e, prop)()
            if not e in r: r.append(e)
        return r

    def __str__(self):
        r=''
        for e in self: r+=str(e)+'\n'
        return r
    def str(self): return str(self)

class field(object):
    def __init__(self, v):
        self._val=v

    def value(self): return self._val

    def __str__(self): return str(self.value())

class columns(col):
    def __init__(self, tbl):
        col.__init__(self)
        self._tbl=tbl

    def add(self, name):
        c=column(name, self._tbl)
        c.ordinal(self.len())
        super(columns, self).add(c)

    def __getitem__(self, ix):
        name=ord=None
        if isinstance(ix, str):
            name=ix
        elif isinstance(ix, int):
            ord=ix

        for c in self:
            if name!=None:
                if c.name() == name:
                    return c
            if ord!=None:
                if c.ordinal() == ord:
                    return c

class column(object):
    def __init__(self, name, tbl):
        self._name=name
        self._ordinal=None
        self._tbl=tbl
    
    def name(self): 
        return self._name

    def ordinal(self, v=None):
        if v!=None: self._ordinal=v
        return self._ordinal


    def fields(self):
        fs=[]
        for r in self._tbl:
            fs.append(r[self.ordinal()])
        return fs

    def max(self):
        m=0
        for f in self.fields():
            l=len(str(f))
            if l > m: m=l
        return m

class row(col):
    """ A collection of field objects."""
    def __init__(self, tbl, *vals):
        col.__init__(self)
        self._tbl=tbl
        self._isheader=False

        if isinstance(vals[0], tuple):
            vals=vals[0]

        for v in vals:
            f=field(v)
            super(row, self).add(f)

    def table(self): return self._tbl

    def isheader(self, v=None):
        if v!=None: self._isheader=v
        return self._isheader

    def __getitem__(self, c):
        if isinstance(c, str):
            c=self.table().columns()[column]
        return super(row, self).__getitem__(c)

class table(col):
    """ A collection of row objects """
    def __init__(self, *hdrs):
        col.__init__(self)
        row=self.add(hdrs)
        row.isheader(True)
        self._columns=None

    def columns(self):
        if not self._columns:
            cs=columns(self)
            hdr=self.header()
            if hdr == None:
                r=self[0]
                if r==None: return None
                rlen=r.len()
            else: rlen=hdr.len()

            for i in range(rlen):
                if hdr!=None: 
                    name=hdr.get(i).value()
                else: name=None
                c=cs.add(name)
            self._columns=cs
        return self._columns

    def sort(self, orderby):
        def gg(c):
            def getter(r):
                return r[c].value()
            return getter

        orderbys = orderby.split(',')
        new=[]
        orderbys.reverse()


        for orderby in orderbys:

            orderby=orderby.split()

            sortorder=None
            if len(orderby) == 2:
                sortorder = orderby[1].lower()

            orderby=orderby[0]

            reverse=sortorder == 'desc'

            ord=self.columns()[orderby].ordinal()

            """ Remove header. Header is needed
            for each iteration to resolve ord."""
            hdr = self.header()
            if hdr: self -= hdr

            self.list.sort(key=gg(ord), 
                           reverse=reverse)

            """ Restore header """
            if hdr: self.unshift(hdr)
            

    def add(self, *vals):
        hdr=self.header()
        if hdr != None:
            if hdr.len() != len(vals):
                msg="Incorrect number of columns"
                raise Exception(msg)
        r=row(self, *vals)
        
        r=super(table, self).add(r, justadd=True)
        return r

    def header(self):
        for r in self:
            if r.isheader(): return r
        return None

    def __str__(self):
        s=''
        for r in self:
            for i, f in enumerate(r):
                max=self.columns()[i].max()
                s += str(f).ljust(max+1)
            s += '\n'
        return s
            
class tree:
    def __init__(self):
        pass

class nodes(col):
    def __init__(self):
        col.__init__(self)

class node(object):
    def __init__(self, value=None, parent=None, tree=None,
                    isnullroot=False):
        self._nodes=nodes()
        self._value=value
        self._parent=parent
        self._tree=tree
        self._isnullroot=isnullroot

    def isnullroot(self, v=None):
        # If true (not the default) it indicates
        # that this node is a root and that its
        # value is inconsequential. Search operations,
        # and so forth will ignore this node if
        # isnullroot is True.
        # It may be useful to add validation here
        # saying 
        # if not self.isroot and self.isnullroot:
        #   throw "this isn't supposed to happen!?"
        if v!=None: self._isnullroot=v
        return self._isnullroot

    def addnode(self, value=None):
        n=node(value, self, self.tree())
        self.nodes().add(n)
        return n

    def nodes(self):
        return self._nodes
    
    def parent(self):
        return self._parent

    def tree(self):
        return self._tree
    
    def value(self):
        return self._value

    def lineage(self, includeself=False):
        ns=nodes()
        if includeself: ns.add(self)
        
        n=self.parent()
        while True:
            if n == None: break
            ns.add(n)
            n=n.parent()
        return ns

    def allnodes(self):
        r=nodes()
        if not self.isnullroot(): r.add(self)
        for n in self.nodes():
            r.add(n.allnodes())

        return r

    def allvalues(self):
        r=[]
        for n in self.allnodes():
            r.append(n.value())
        return r

    def contains(self, value):
        # TODO: the 'value' param could be a node
        # if we implemented for it.
        return value in self.allvalues()

    def __repr__(self, recursive=True):
        if not recursive:
            r=str(self.value())
        else:
            r=self.__repr__(False)
            indent='' 
            for n in self.lineage(includeself=True):
                indent+=(len(str(n.value())) + 1) * ' '

            for n in self.nodes():
                r+="\n" + indent + '-'

                r+=n.__repr__(True)

        return r

class BrokenRules(col):
    def __init__(self):
        col.__init__(self)

    def add(self, e):
        if isinstance(e, str):
            br=BrokenRule(e)
        elif isinstance(e, BrokenRule):
            br=e
        else:
            if hasattr(e, "brokenrules"):
                br=e.brokenrules()
            else:
                raise ValueError("Unsupported type for adding to brokenrules")

        col.add(self, br)

    def __str__(self):
        msg=''
        for br in self: msg+=str(br) + "\n"
        return msg

class BrokenRule:
    def __init__(self, msg):
        self._msg=msg
    def __str__(self): return self._msg
    def __repr__(self): return str(self)
        
        
    

    


        



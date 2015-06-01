#!/usr/bin/env python

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

import yaml
import os
from dt import *
import inspect
b=pdb.set_trace

class Candies(col):
    def __init__(self, v, candy, parent=None):
        col.__init__(self)
        # This is a temp hack
        # so services can be 
        # used as plain old collection class
        if v == None: return
        o=None
        self._candy = candy
        self._parent = parent
        self._path=None
        if isinstance(v, str):
            v={v: None}

        for k,v in v.iteritems():

            candy = self.getcandy(v)


            o = candy(v, self)

            o._key=k

            if not o:
                print("Candy object not set")
                os._exit(1)
            self.add(o)

    def brokenrules(self):
        brs=BrokenRules()
        keys={}
        for o in self:
            try:
                keys[o.key()]
                brs.add('Duplicate key (%s) for %s' % (o.key(), o.path()))
            except KeyError: pass
            
            brs.add(o)
        return brs

    def isvalid(self):
        return brs.len() == 0

    def parent(self): return self._parent

    def getcandy(self, v):
        if v == None: return self._candy
        try: 
            candy=v['_type']
        except TypeError: 
            print "Error in yaml: %s" % v
            os._exit(1)
        except KeyError: 
            return self._candy

        clss=Candy.candyclasses()

        try:
            return clss[candy]
        except KeyError:
            print "Candy object from yaml not found: %s" % v
            print "Found in object %s" % self
            os._exit(1)

    _candiesclasses=None
    @staticmethod
    def candiesclasses():
        if Candies._candiesclasses == None:
            Candies._candiesclasses = collectallsubclasses(Candies)
        return Candies._candiesclasses

class Candy(object):
    def __init__(self, data, parent):
        self._parent = parent
        self._props={}
        self._main=None
        self._path=None
        if data:
            for k,v in data.iteritems():
                if k == '_type': continue
                fn = self.getfn(k)
                if fn == None:
                    msg="No attribute '%s' for class '%s'." % (k, self.__class__.__name__)
                    print msg
                    os._exit(1)
                fn(v)

    def brokenrules(self):
        brs=BrokenRules()
        cls=self.__class__
        if hasattr(cls, 'brokenifNone'):
            for method in cls.brokenifNone.split():
                fn=self.getfn(method)
                if fn == None:
                    msg="brokenifNone test for method %s failed because doesn't exist" % method
                    raise Exception(msg)
                v = fn()
                if v == None:
                    msg="Schema Fail: "
                    msg+="%s must have '%s' node" % (self.path(), method)
                    brs.add(msg)
        return brs

    def isvalid(self):
        return self.brokenrules.len() == 0

    def _type(self):
        return self.__class__.__name__

    def path(self):
        if not self._path:
            p = ""

            rent=self.parent()
            key=self.key()

            while True:
                rentname = rent.__class__.__name__
                p = "/%s[%s]%s" % (rentname, key, p)

                rent=rent.parent()
                if not rent: break

                key = rent.key()
                rent=rent.parent()

            self._path=p
        
        return self._path
            
    def _ref(self, v=None):
        return self.prop(v)

    def isreference(self):
        return self._ref() != None

    def prop(self, v=None, parent=None):
        collection=parent != None
        prop = inspect.stack()[1][3]
        if v != None: 
            if collection:
                self._props[prop] = Candies.candiesclasses()[prop](v, parent)
            else:
                self._props[prop] = v
        try:
            return self._props[prop]
        except KeyError:
            main = self._main
            try:
                if main:
                    return main._props[prop]   
            except KeyError:
                return None
            
    def candiescollection(self):
        r=[]
        for v in self._props.values():
            if isinstance(v, Candies):
                r.append(v)
        return r

    def parent(self): return self._parent 
    def getfn(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return None

    def key(self, v=None): 
        return self._key

    def __str__(self): return self.path()
    def __repr__(self): return str(self)

    _candyclasses=None
    @staticmethod
    def candyclasses():
        if Candy._candyclasses == None:
            Candy._candyclasses = collectallsubclasses(Candy)
        return Candy._candyclasses

def collectallsubclasses(cls, collection=None):
    clss=cls.__subclasses__()

    isroot = collection == None
    if isroot: collection={}

    for cls in clss:
        collection[cls.__name__]=cls
        collectallsubclasses(cls, collection)

    if isroot: return collection

class CandyStore(col):
    def __init__(self, src):
        self._top = {}
        self._sources = []
        self._all=None

        # TODO: Fully support multiple sources (yaml files)
        self._sources.append(src)

        collections = yaml.load(file(src))

        for collection in collections:
            for k,v in collection.iteritems():
                o=Candies.candiesclasses()[k](v)
                self.add(k, o)
        self.resolvereferences()

    def brokenrules(self):
        brs=BrokenRules()
        for o in self.top():
            brs.add(o)
        return brs

    def isvalid(self):
        return self.brokenrules().len() == 0

    def demandvalid(self):
        if self.isvalid(): return True

        print str(self.brokenrules())

        return False
            
            

    def add(self, name, obj):
        self._top[name]=obj

    def resolvereferences(self):
        for o in self.allcandy():
            if o.isreference():
                main = o._ref()
                o._main = self.get(main)

    def get(self, path):
        path=path.strip()
        for o in self.all():
            if o.path() == path:
                return o
        return None

    def all(self):
        if not self._all:
            self.collectall()
        return self._all

    def collectall(self, col=None):
        if col == None:
            col = self.top()
            self._all=[]

        for o in col:
            if isinstance(o, Candies):
                self.collectall(o)
            elif isinstance(o, Candy):
                for o in o.candiescollection():
                    self.collectall(o)
            self._all.append(o)

    def allcandy(self):
        r=[]
        for o in self.all():
            if isinstance(o, Candy):
                r.append(o)
        return r

            
    def top(self):
        return self._top.values()

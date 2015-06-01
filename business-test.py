#!/bin/python

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

from business import business, nochg

class users(col): pass
class user(business):
    def __init__(self, id=None, override=False):
        business.__init__(self, id)

    zrm_name="varchar(250) not null"
    def name(self, v=None):
        return self.prop(v)

    zrm_active="bit default 1"
    def active(self, v=nochg):
        return self.prop(v)

class orders(col): pass
class order(business):
    zrm_fk="user.id"
    zrm_tablename="order_"
    def __init__(self, id=None)
        business.__init__(self, id)

    zrm_type="int default 1"
    def active(self, v=nochg):
        return self.prop(v)

class test:
    def __init__(self):
        self._file='/tmp/my.db'

    def createtables(self):
        conns = self.connections()
        # conns.drop()
        conns.createtables()

    def connections(self):
        bom=business_object_manager.getinstance()
        bom.connections().add(self._file)

    def run(self):
        self.createtables()



t=test()
t.run()

    


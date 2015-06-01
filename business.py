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
from dt import *
import sqlite3
import inspect
class business_object_manager:
    def __init__(self, override=False):
        if not override:
            msg='business_object_classes is a singlton. '
            msg+='Use business_object_classes.getinstance() instead.'
            raise NotImplementedError(msg)
        self._conns=connections()

    bom=None

    @staticmethod
    def getinstance():
        if business_object_manager.bom == None:
            business_object_manager.bom=\
                business_object_manager(override=True)
        return business_object_manager.bom

    def connections(self):
        return self._conns

    
    def business_object_classes(self):
        return business.__subclasses__()

class connections(col):
    def add(self, conn):
        if isinstance(conn, str):
            # if connstr is sqlite:
            conn=sqlite_connection(conn)
            col.add(self, conn)

    def createtables(self):
        for conn in self:
            conn.createtables()

    def current(self):
        """ This needs to be a setter. """
        len = self.len()
        if len == 1:
            return self[0]
        elif len == 0:
            return None
            
            
class connection(object):
    def __init__(self, connstr):
        self._connstr=connstr
        self._dbapi_conn=None

    def bom(self):
        return business_object_manager.getinstance()

    def connstr(self):
        return self._connstr

    def tables(self):
        raise NotImplementedError("tables is abstract")

    def dbapi_conn(self):
        raise NotImplementedError("dbapi_conn is abstract")

    def createtables(self):
        for tbl in self.tables():
            ddl=tbl.CREATE()
            try:
                self.exe(ddl)
            except sqlite3.OperationalError as err:
                msg = "Error creating table '%s'.\n" % tbl.name()
                msg += "Possible causes include:\n"
                msg += "\tThe table name is reserved. "
                msg += "Consider the 'zrm_tablename' attribute."
                b()
                raise ZORMError(msg, err)

    def exe(self, sql):
        conn=self.dbapi_conn()
        c=conn.cursor()
        c.execute(sql)
        conn.commit()
        return c

class sqlite_connection(connection):
    def __init__(self, connstr):
        connection.__init__(self, connstr)
        self._tbls = None


    def file(self):
        return self.connstr()

    def dbapi_conn(self):
        if self._dbapi_conn == None:
            self._dbapi_conn=sqlite3.connect(self.file())
        return self._dbapi_conn

    def tables(self):
        if self._tbls == None:
            self._tbls=tables()
            for bo in self.bom().business_object_classes():
                
                tbl=sqlite_table(bo._business__zrm_table(bo))
                str(tbl)
                self._tbls.add(tbl)
        return self._tbls


class business(object):
    __zrm_tbl=None
    __zrm_cols=None

    def __init__(self, id=None):
        self._id=id
        self._isdirty=False
        self._props={}
        self.load()

    def prop(self, v):
        prop = inspect.stack()[1][3]
        if v != nochg: 
            if not self._isdirty:
                if prop in self._props.keys():
                    self._isdirty = self._props[prop] != v
            self._props[prop] = v
            return v
        else:
            try:
                return self._props[prop]
            except:
                return None

    def load(self):
        if self._id == None: return
        cls=self.__class__
        cols=cls._business__zrm_columns(cls)

        c = self.exe(self.SELECT())

        for r in c:
            for i, desc in enumerate(c.description):
                colname = desc[0]
                # TODO: 'id' shouldn't be hardcoded
                if colname == 'id': continue 
                col = cols.get(colname)
                mut = col.getmutator()
                v = col.db2py(r[i])

                mut(self, v)
        

    def id(self): return self._id

    @staticmethod
    def __zrm_table(cls):
        tbl = cls._business__zrm_tbl
        if tbl == None:
            name = getattr(cls,
                            'zrm_tablename',
                            cls.__name__)

            name = cls.__name__
            tbl=table(name=name)
            tbl.columns(cls._business__zrm_columns(cls))
            cls._business__zrm_tbl = tbl
        return tbl

    @staticmethod
    def __zrm_columns(cls):
        cols = cls.__zrm_cols
        if not cols:
            Prefix='zrm_'
            FK=Prefix + 'fk'
            cols=columns(withPK=True, cls=cls)
            for var in cls.__dict__.keys():
                if var.startswith(Prefix):

                    if var in (Prefix + 'tablename'): continue

                    val=cls.__dict__[var]
                    if isinstance(val, str):
                        if var != FK:
                            name=var[len(Prefix):]
                            col=column(name, val, cls=cls)
                        else:
                            for fk in val.split():
                                col=column(fk=fk)
                        cols.add(col)
                    else:
                        msg="ORM clauses must be string types. Check: %s" % var
                        raise ZORMError(msg)
            cls.__zrm_cols = cols
        return cols

    def bom(self):
        return business_object_manager.getinstance()

    def isvalid(self):
        return self.brokenrules().len() == 0

    def brokenrules(self):
        # TODO
        r = col()
        return r

    def isnew(self):
        return self._id == None
        
    def isdirty(self):
        if self.isnew():
            return True
        else:
            return self._isdirty

    def __zrm_update(self):
        sql = 'update %s' % self.tablename()
        for col in business.__zrm_columns():
            pass

    def tablename(self):
        return self.__class__.__name__

    def SELECT(self):
        sql = """select *
                    from %s
                    where id = %s""" 
        return sql % (self.tablename(), self.id())

    def INSERT(self):
        sql = 'insert into %s (' % self.tablename()
        beenhere=False
        cls=self.__class__

        cols = cls._business__zrm_columns(cls)
        
        for c in cols:

            if beenhere: sql += ', '
            else: beenhere=True

            sql += c.name()

        sql += ') values('

        beenhere = False
        for c in cols:

            if beenhere: sql += ', '
            else: beenhere=True

            if c.pk(): sql += 'null'
            else: sql += str(c.getval(self, True))

        return sql + ")"

    def _insert(self):
        self.exe(self.INSERT())

    def save(self):
        if not self.isvalid():
            msg="%s is invalid" % self.__name__
            raise BrokenRulesError(msg, self)

        if self.isdirty():
            if self.isnew():
                self._insert()
            else:
                self._update()

    def exe(self, sql, conn=None):
        cur = self.bom().connections().current()
        return cur.exe(sql)

class tables(col):
    pass

class table:
    def __init__(self, name=None, tbl=None):
        if name != None: self._name=name

        if tbl == None:
            self._columns=columns()
        else:
            self._name=tbl.name()
            self.columns(tbl.columns())

    def CREATE(self):
        raise NotImplementedError("CREATE is abstract")

    def columns(self, v=None):
        if v != None: self._columns=v
        return self._columns

    def name(self):
        return self._name

    def brokenrules(self):
        r=[]
        if self.columns().len() == 0:
            r.append('Table [%s] has no columns' % self.name())
        return r

    def isvalid(self):
        return len(self.brokenrules()) == 0

    def demandvalid(self):
        if not self.isvalid():  
            # todo: Create Error that can report brokenrules
            raise Exception('Table [%s] is not valid' % self.name())

    @staticmethod
    def tosql(col):
        r = col.name() + ' '
        r += col.type().upper() + ' '
        if col.len() != None:
            r += '(%s) ' % col.len()
        if col.pk(): r += "PRIMARY KEY "

        # TODO: Add optional referential integrity
        # for FK:
        # if col.fk(): r+= "foreign key(col.name()) 
        #                   references col.reftable(col.superkey())"

        if col.notnull(): r += "NOT NULL "
        if col.unique():  r += "UNIQUE "

        df = col.default()
        if df != None:
            if col.isquotable():
                df = "'%s'" % df
                
            r += "DEFAULT %s" % df
        return r.rstrip()

    def __str__(self):
        CREATE=self.CREATE()
        print(CREATE)
        return CREATE

class sqlite_table(table):
    def __init__(self, tbl):
        table.__init__(self, tbl=tbl)

    def CREATE(self):
        table.demandvalid(self)

        r='create table %s(' % self.name()
        indent=len(r) * ' '

        cols=self.columns()
        for col in cols:
            r += '\n' + indent
            r += table.tosql(col) + ','
        r = r[:-1]
        r += ')'
        return r

class columns(col):
    def __init__(self, withPK=False, cls=None):
        col.__init__(self)
        self._cls=cls

        if withPK:
            c=column(pk=True, cls=cls)
            self.add(c)


    def get(self, by):
        if isinstance(by, str):
            for col in self:
                if col.name() == by:
                    return col
            raise ZORMError("Column %s not found" % by)
        else:
            return col.get(by)

    def __str__(self):
        r=''
        for c in self:
            r += str(c) + '\n'
        return r

    def __repr__(self):
        return str(self)



class column:
    def __init__(self, name=None,
                            clause=None,
                            pk=False,
                            cls=None,
                            fk=None):
        # TODO: param validation
        self._cls=cls
        self._pk=pk
        self._fk=fk
        if  not (pk or fk):
            self._name=name
            self._type=None
            self._uniq=False
            self._df=None
            self._notnull=False
            self._len=None
            self.parseclause(clause)

    def fk(self): return self._fk != None

    def getmutator(self):
        # TODO: demand cls is not None
        return getattr(self._cls, self.name())

    def pk(self): return self._pk

    def getval(self, bo, quoteForString):
        name = self.name()
        accessor = bo.__getattribute__(name)
        val = accessor() 
        if quoteForString and self.isquotable():
            val = "'" + val + "'"
        if self.type() == 'bit':
            val = (0,1)[val]
        return val
        


    @staticmethod
    def bit(x): 
        if x not in (0,1):
            raise ValueError()

    def brokenrules(self):
        r=[]
        t=self.type()
        df=self.default()
        if not self.istypevalid():
            r.append("Invalid type: %s" % t)
        if self.needlen() and self.len() == None:
            r.append("Datatype '%s' needs length/scale data" % t)

        if df != None:
            if self.istypenumeric():
                if   t == 'integer':   f=int
                elif t == 'float':     f=float
                elif t == 'bit':       f=business.bit
                try: f(df)
                except ValueError:
                    r.append("Default value is not an %s: %s" % (t,df))
        return r
                    
    def isvalid(self):
        return self.brokenrules().len() == 0

    def name(self): 
        if self.pk(): 
            return 'id'
        if self.fk():
            return self._fk.replace('.', '_')
        return self._name

    def db2py(self, v):
        t=self.type()
        if t not in ('datetime', 'bit'): return v
        elif t == 'bit': return bool(v)

    def isquotable(self):
        return not self.istypenumeric()

    def istypevalid(self):
        return self.type() in \
            ('integer', 'float', 'varchar', 
            'text', 'blob', 'datetime',
            'bit')
    def istypenumeric(self):
        return self.type() in ('integer', 'float', 'bit')

    def type(self, v=None):
        self.demandPKFalseForMutatation(v != None)
        if v: self._type=v

        if self.iskey(): return 'integer'

        if self._type != None:
            self._type=self._type.strip().lower()
            if self._type == 'int': 
                self._type='integer'
        return self._type

    def unique(self, v=None):
        self.demandPKFalseForMutatation(v != None)
        if v != None: self._uniq=v
        if self.pk(): return True
        elif self.fk(): return False
        return self._uniq


    def default(self, v=None):
        # Note: we can't use this to
        # set back to None if we ever
        # need to.
        self.demandPKFalseForMutatation(v != None)
        if self.iskey(): return None
        if v: self._df=v
        return self._df

    def demandPKFalseForMutatation(self, cond):
        if cond and self.pk():
            curframe = inspect.currentframe()
            calframe = inspect.getouterframes(curframe, 2)
            meth=calframe[1][3]
            msg = "Can't set '%s' when self.pk() == True" % (meth)
            raise ValueError(msg)

    def notnull(self, v=None):
        self.demandPKFalseForMutatation(v != None)
        if v != None: self._notnull=v
        if self.iskey(): return True
        return self._notnull

    def iskey(self): return self.pk() or self.fk()

    def len(self, v=None):
        self.demandPKFalseForMutatation(v != None)

        if self.iskey(): return None
            
        if v != None: self._len=v
        return self._len

    def __str__(self):
        r = "%s: " % self.name()
        r += self.type().upper()
        if self.len() != None: r += "(%s)" % self.len()
        if self.pk(): r += " PRIMARY KEY"
        if self.unique(): r += " UNIQUE"
        if self.notnull(): r += " NOT NULL"
        if self.default() != None: 
            r += " " + "DEFAULT "
            if self.istypenumeric():
                r += "%s" % self.default()
            else:
                r += "'%s'" % self.default()
        return r

    def __repr__(self):
        return str(self)

    def needlen(self):
        return self.type() in \
            ('varchar', 'char', 'decimal')

    def parseclause(self, clause):
        buf=''
        collect_df_arg=collect_not_arg=False
        isquote=quotefound=inquote=False
        close_paren_found=False
        open_paren_found=False
        collect_len=False
        inparen=False

        clause += ' '
        for c in clause.lower():
            isspace=c.isspace()
            if isspace or open_paren_found:
                if self.type() == None and buf != '': 
                    self.type(buf.rstrip("("))
                    if not self.istypevalid():
                        raise ParseError("Invalid type: %s" % self.type())
                    collect_len=self.needlen()
                    buf=''
                elif collect_len:
                    if close_paren_found:
                        if not inparen:
                            raise ParseError("missing (")
                        buf=buf.strip()
                        self.len(buf.strip(")("))
                        close_paren_found=False
                        collect_len=False
                        inparen=False
                        buf=''
                elif collect_df_arg:
                    if ((not inquote) or (inquote and quotefound)) and buf != '':
                        if inquote: 
                            buf=buf[1:-1]
                            buf=buf.replace("''", "'")
                        self.default(buf)
                        collect_df_arg=False
                        quotefound=inquote=False
                        buf=''
                elif collect_not_arg:
                    if buf == 'null':
                        self.notnull(True); collect_not_arg=False
                        buf=''
                    elif buf != '': 
                        raise ParseError("not requires null")

                elif buf == 'default': 
                    collect_df_arg=True; buf=''
                elif buf == 'unique':  
                    self.unique(True); buf=''
                elif buf == 'not':     collect_not_arg=True; buf=''
                elif buf != '':
                    raise ParseError("unidentified token: %s" % buf)
                open_paren_found=False
            else:
                close_paren_found = c == ")"
                if c == "(":
                    open_paren_found=True
                    inparen=True

                isquote = c == "'"
                if not inquote: inquote = isquote
                else:
                    if quotefound:
                        if isquote: quotefound=False # esc
                    else:
                        quotefound = isquote

            if inquote or not isspace:
                buf += c

        if collect_df_arg: raise ParseError("Default argument not found")
        if collect_not_arg:raise ParseError("NOT argument not found")
        if collect_len:    raise ParseError("Length required for %s type" % self.type())
        if buf != '':      raise ParseError("Error parsing at %s" % buf)

class nochg: pass
class ZORMError(Exception): 
    def __init__(self, msg, ex):
        self.inner=ex
        Exception.__init__(self, msg)

class BrokenRulesError(ZORMError): pass
class ParseError(ZORMError): pass

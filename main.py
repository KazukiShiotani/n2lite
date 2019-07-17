#!/usr/bin/env python3

import sqlite3
import pandas
import re

class N2lite():
    
    def __init__(self, dbpath):
        """
        example:
            dbpath = "/home/amigos/data/logger/sample.db"
        """
        self.dbpath = dbpath
        self.con = sqlite3.connect(self.dbpath, check_same_thread=False)
        pass
    
   # def __del__(self):
   #     self.con.close()
   #     return
    
    def open(self):
        """
        for multithread, because sqlite cannnot connect beyond thread.
        """
        self.con = sqlite3.connect(self.dbpath, check_same_thread=False)

    def close(self):
        self.con.close()
        return
    
    def commit_data(self):
        self.con.commit()
        return

    def make_table(self, table_name, param):
        """
        example:
            table_name = "SIS_VOLTAGE"
            param = {"param1":"float", "param2":"float"}
        """
        sql_param = str(param)[1:-1]
        sql_param = re.sub(":","",sql_param)
        sql_param = "({})".format(sql_param)
        self.con.execute("CREATE table if not exists {} {}".format(table_name, sql_param))
        pass

    def write(self, table_name, param, values, auto_commit = False):
        """
        example:
            table_name = "SIS_VOLTAGE"
            param = "('2l', '2r')" or '' (all param write)
            values = "(1.0, 2.0)" or [1.0, 2.0]

            if autocommit = False, you must call commit_data function 
                after calling write function.
        """
        if isinstance(values,list):
            values = tuple(values)
        if len(values) == 1:
            values = "({})".format(values[0])
        if auto_commit:
            with self.con:
                self.con.execute("INSERT into {0} {1} values {2}".format(table_name, param, values))
        else:
            self.con.execute("INSERT into {0} {1} values {2}".format(table_name, param, values))
        return
    
    def bytes_write(self, table_name, param, data, auto_commit = False):
        sql = "insert into {0} {1} values (?,?)".format(table_name, param)
        self.con.executemany(sql, data)
        return

    def writemany(self, table_name, param, values, auto_commit = False):
        """
        example:
            table_name = "SIS_VOLTAGE"
            param = "('2l', '2r')" or '' (all param write)
            values = "(1.0, 2.0), (1.1, 2.2)"

            if autocommit = False, you must call commit_data function 
                after calling write function.

            if value's type = list, need  ",".join(map(str, value)).
        """
        
        if auto_commit:
            with self.con:
                self.con.execute("INSERT into {0} {1} values {2}".format(table_name, param, values))
        else:
            self.con.execute("INSERT into {0} {1} values {2}".format(table_name, param, values))
        return

    def read(self, table_name, param="*"):
        """
        example:
            table_name = "SIS_VOLTAGE"
            param = "'2l', '2r', time" or "*" (all param read)
        return:
            [[1.2, 1.3, .....], [...]]
        """
        row = self.con.execute("SELECT {0} from {1}".format(param, table_name)).fetchall()
        if not row == []:
            data = [
                [row[i][j] for i in range(len(row))] 
                    for j in range(len(row[0]))
                    ]
        else : data = []
        return data

    def read_as_pandas(self, table_name, where = ""):
        """
        example:
            table_name = "SIS_VOLTAGE"
        return:
            pandas.core.frame.DataFrame
        """
        if where == "":
            sql = "SELECT * from {}"
            df = pandas.read_sql(sql.format(table_name), self.con)
        else:
            sql = "SELECT * from {} where {}"
            df = pandas.read_sql(sql.format(table_name, where), self.con)
        return df

    def read_pandas_all(self):
        table_name = self.get_table_name()
        datas = [self.read_as_pandas(name) for name in table_name]
        if datas ==[]:
            df_all = []
        else:
            df_all = pandas.concat(datas, axis=1)
        return df_all

    def check_table(self):
        """
        get information about all table
        example:
            type = table
            name = SIS_VOLTAGE
            tbl_name = SIS_VOLTAGE
            rootpage = 3
            sql = CREATE TABLE SIS_VOLTAGE ('2l', '2r')
        """
        row = self.con.execute("SELECT * from sqlite_master")
        info = row.fetchall()
        return info

    def get_table_name(self):
        """
        get names of tables
        example:
            ["SIS_VOLTAGE", "time", ...]
        """
        name = self.con.execute("SELECT name from sqlite_master where type='table'").fetchall()
        name_list = [name[i][0] for i in range(len(name))]
        return name_list

class xffts_logger(N2lite):
    def __init__(self, dbpath):
        super().__init__(dbpath)
        pass
    

    def write_blob(self, table_name, param, auto_commit = False):#tmp
        if auto_commit:
            with self.con:
                self.con.execute("insert into {} values (?,?)".format(table_name), param)
        else:
            self.con.execute("insert into {} values (?,?)".format(table_name), param)

    def write_blob2(self, table_name, param, auto_commit = False):#tmp
        if auto_commit:
            with self.con:
                self.con.executemany("insert into {} values (?,?,?,?)".format(table_name), param)
        else:
            self.con.executemany("insert into {} values (?,?,?,?)".format(table_name), param)

    def write_blob3(self, table_name, param, auto_commit = False):#tmp
        if auto_commit:
            with self.con:
                self.con.executemany("insert into {} values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)".format(table_name), param)
        else:
            self.con.executemany("insert into {} values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)".format(table_name), param)
    def read_as_timestamp(self, table_name, where, param="*"):
        #row = self.con.execute("SELECT {0} from {1} where timestamp < {2} and timestamp > {3}".format(param, table_name, where)).fetchall()
        row = self.con.execute("SELECT {0} from {1} where timestamp < {2}".format(param, table_name, where)).fetchall()
        if not row == []:
            data = [
                [row[i][j] for i in range(len(row))] 
                    for j in range(len(row[0]))
                    ]
        else : data = []
        return data
        


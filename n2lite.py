#!/usr/bin/env python3

import sqlite3
import pandas

__version__ = "0.4.0"


class N2lite(object):
    
    def __init__(self, dbpath):
        """
        example:
            dbpath = "/home/amigos/data/logger/sample.db"
        """
        self.dbpath = dbpath
        self.con = sqlite3.connect(self.dbpath, check_same_thread=False)
        pass
    
    def __del__(self):
        self.con.close()
        return
    
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
            param = "('2l' float, '2r' float, time float, ... )"
        """
        self.con.execute("CREATE table if not exists {} {}".format(table_name, param))
        return

    def write(self, table_name, param, values, auto_commit = False):
        """
        example:
            table_name = "SIS_VOLTAGE"
            param = "('2l', '2r')" or '' (all param write)
            values = "(1.0, 2.0)"

            if autocommit = False, you must call commit_data function 
                after calling write function.
        """
        if auto_commit:
            with self.con:
                self.con.execute("INSERT into {0} {1} values {2}".format(table_name, param, values))
        else:
            self.con.execute("INSERT into {0} {1} values {2}".format(table_name, param, values))
        return
    
    def bytes_write(self, table_name, param, data, auto_commit = False):
        sql = "insert into {0} {1} values (?,?)".format(table_name, param)
        self.con.exectemany(sql, data)
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

    def read_as_pandas(self, table_name):
        """
        example:
            table_name = "SIS_VOLTAGE"
        return:
            pandas.core.frame.DataFrame
        """
        df = pandas.read_sql("SELECT * from {}".format(table_name), self.con)
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

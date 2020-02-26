# -*- coding: utf-8 -*-
#__author__ = 'liuyuzhang'

from impala.dbapi import connect


class db_hv:
    def __init__(self, host, db, user, pwd, port, auth):
        self.host = host
        self.db = db
        self.user = user
        self.pwd = pwd
        self.port = port
        self.auth = auth
        self._conn = self._connect()
        self._cursor = self._conn.cursor()

    def _connect(self):
        return connect(
            database=self.db,
            user=self.user,
            password=self.pwd,
            host=self.host,
            port=self.port,
            auth_mechanism=self.auth)

    def select(self, sqlCode):
        self.common(sqlCode)
        col_names = []
        result = {}
        column_count = len(self._cursor.description)
        for i in range(column_count):
            desc = self._cursor.description[i]
            col_names.append(desc[0])
        data = self._cursor.fetchall()
        result['head'] = col_names
        result['data'] = data
        return result

    def close(self):
        self._cursor.close()
        self._conn.close()

    def common(self, sqlCode):
        try:
            self._cursor.execute(sqlCode)
        except Exception as e:
            print(e)
            self._conn.rollback()
            self._cursor.execute(sqlCode)
        self._conn.commit()

    def __del__(self):
        self.close()
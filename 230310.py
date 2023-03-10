import bs4
import pandas as pd
import requests
import cx_Oracle
import pymysql

# 오라클
dsn = cx_Oracle.makedsn('localhost', 1521, 'xe')
oracle_conn = cx_Oracle.connect(user='icia', password='1234', dsn=dsn)
sql_oracle = oracle_conn.cursor()

oracle_query = 'create table test3(id number(1), name varchar(5), constraint test115_pk primary key(id))'
sql_oracle.execute(oracle_query)

oracle_query2 = 'select * from test3'
pd.read_sql(sql=oracle_query2, con=oracle_conn)

oracle_query3 = "insert into test3(id, name) values(3, 'test3')"
sql_oracle.execute(oracle_query3)

oracle_conn.commit()
sql_oracle.close()
oracle_conn.close()

# mysql
sql_conn = pymysql.connect(host='localhost', user='root', password='1234', db='lol_icia', charset='utf8')
sql_mysql = sql_conn.cursor()

sql_query = 'create table test_mysql2(id smallint, name varchar(5), PRIMARY KEY(id))'
sql_mysql.execute(sql_query)

sql_query2 = "insert into test_mysql2(id, name) values(3, 'test3')"
sql_mysql.execute(sql_query2)
result = sql_mysql.fetchall()
sql_conn.commit()

sql_query3 = 'select * from test_mysql2'
sql_mysql.execute(sql_query3)
result = sql_mysql.fetchall()
# sql_conn.close()
print(result)

sql_query4 = 'insert into test_mysql2(id, name) values(3, "test3") on duplicate key update id = 3, name = "test4"'
sql_mysql.execute(sql_query4)
result = sql_mysql.fetchall()
sql_conn.commit()

import my_utils as mu
import imp

imp.reload(my_utils)

mu.oracle_open()

oracle_query = 'select * from test3'
mu.oracle_execute(oracle_query)

oracle_query3 = "insert into test3(id, name) values(4, 'test4')"
mu.oracle_execute(oracle_query3)

mu.oracle_close()

sql_conn = mu.connect_mysql('lol_icia')

mu.mysql_execute('select * from test_mysql2', sql_conn)
mu.mysql_execute_dict('select * from test_mysql2', sql_conn)

mu.mysql_execute('insert into test_mysql2(id, name) values(3, "test3") on duplicate key update id = 3, name = "test3"', sql_conn)
sql_conn.commit()
sql_conn.close()

url = 'http://openapi.seoul.go.kr:8088/(인증키)/xml/tbLnOpendataRentV/1/5/'
mu.df_creater(url)
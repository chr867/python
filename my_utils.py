import pandas as pd
import pymysql
import cx_Oracle
import requests

dsn = cx_Oracle.makedsn('localhost', 1521, 'xe')
seoul_api_key = '77676c6c566368723431555642674c'


def oracle_open():
    global oracle_conn
    global oracle_cursor
    oracle_conn = cx_Oracle.connect(user='icia', password='1234', dsn=dsn)
    oracle_cursor = oracle_conn.cursor()
    print('oracle open!')


def oracle_execute(q):
    global oracle_conn
    global oracle_cursor
    try:
        if 'select' in q:
            df = pd.read_sql(sql=q, con=oracle_conn)
            return df
        oracle_cursor.execute(q)
        return 'oracle 쿼리 성공'
    except Exception as e:
        print(e)


def oracle_close():
    global oracle_conn
    global oracle_cursor
    try:
        oracle_conn.commit()
        oracle_cursor.close()
        oracle_conn.close()
        return '오라클 닫힘'
    except Exception as e:
        print(e)


'''
mysql
'''


def connect_mysql(db):
    mysql_conn = pymysql.connect(host='localhost', user='root', password='1234', db=db, charset='utf8')
    return mysql_conn


def mysql_execute(query, mysql_conn):
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(query)
    result = mysql_cursor.fetchall()
    return result


def mysql_execute_dict(query, mysql_conn):
    mysql_cursor = mysql_conn.cursor(cursor=pymysql.cursors.DictCursor)
    mysql_cursor.execute(query)
    result = mysql_cursor.fetchall()
    return result


def df_creater(url):
    url = url.replace('(인증키)', seoul_api_key).replace('xml', 'json').replace('/5/', '/1000/')
    res = requests.get(url).json()
    key = list(res.keys())[0]
    data = res[key]['row']
    df = pd.DataFrame(data)
    return df
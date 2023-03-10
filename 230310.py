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
df = mu.df_creater(url)
seoul_df = df

sel_c = ['SGG_NM', 'BJDONG_NM', 'LAND_GBN_NM', 'FLR_NO', 'CNTRCT_DE', 'RENT_GBN', 'RENT_AREA', 'RENT_GTN', 'RENT_FEE', 'BLDG_NM', 'BUILD_YEAR']
seoul_df = df[sel_c]
seoul_df.columns=['자치구명', '법정동명', '지번구분명', '층수', '계약일', '전월세', '면적', '보증금', '임대료', '건물명', '건축년도']
seoul_df = seoul_df.astype({'층수': 'float', '계약일': 'int', '면적': 'float', '보증금': 'int', '임대료': 'int'})

# ORACLE CREATE 테이블
oracle_query = """ 
CREATE TABLE housePrice(자치구명 varchar(50), 법정동명 varchar(50), 지번구분명 varchar(50), 층수 float,
계약일 int, 전월세 varchar(20), 면적 float(20), 보증금 number(20), 임대료 number(20), 건물명 varchar(100),
건축년도 varchar(20))
"""
mu.oracle_open()
mu.oracle_execute(oracle_query)

# mysql CREATE 테이블
sql_query = """
CREATE TABLE housePrice(자치구명 varchar(50), 법정동명 varchar(50), 지번구분명 varchar(50), 층수 float,
계약일 int, 전월세 varchar(20), 면적 float(20), 보증금 int, 임대료 int, 건물명 varchar(100), 건축년도 varchar(20))
"""
sql_conn = mu.connect_mysql('lol_icia')
mu.mysql_execute(sql_query, sql_conn)

# ORACLE INSERT
oracle_query2 = (
    f'insert into housePrice(자치구명,법정동명,지번구분명,층수,계약일,전월세,면적,보증금,임대료,건물명,건축년도)'
    f'values({repr(seoul_df.자치구명.iloc[0])},{repr(seoul_df.법정동명.iloc[0])},{repr(seoul_df.지번구분명.iloc[0])},'
    f'{seoul_df.층수.iloc[0]},{seoul_df.계약일.iloc[0]},{repr(seoul_df.전월세.iloc[0])},{seoul_df.면적.iloc[0]},'
    f'{seoul_df.보증금.iloc[0]},{seoul_df.임대료.iloc[0]},{repr(seoul_df.건물명.iloc[0])},{repr(seoul_df.건축년도.iloc[0])})'
)
mu.oracle_open()
mu.oracle_execute(oracle_query2)
mu.oracle_close()

# mysql INSERT
sql_conn = mu.connect_mysql('lol_icia')
sql_query2 = (
    f'insert into housePrice(자치구명,법정동명,지번구분명,층수,계약일,전월세,면적,보증금,임대료,건물명,건축년도)'
    f'values({repr(seoul_df.자치구명.iloc[0])},{repr(seoul_df.법정동명.iloc[0])},{repr(seoul_df.지번구분명.iloc[0])},'
    f'{seoul_df.층수.iloc[0]},{seoul_df.계약일.iloc[0]},{repr(seoul_df.전월세.iloc[0])},{seoul_df.면적.iloc[0]},'
    f'{seoul_df.보증금.iloc[0]},{seoul_df.임대료.iloc[0]},{repr(seoul_df.건물명.iloc[0])},{repr(seoul_df.건축년도.iloc[0])})'
)
mu.mysql_execute(sql_query2, sql_conn)
sql_conn.commit()
sql_conn.close()

sql_conn = mu.connect_mysql('lol_icia')
sql_query3 = 'select * from housePrice'
mu.mysql_execute(sql_query3, sql_conn)


# insert 문을 함수로
from tqdm import tqdm
# ORACLE
mu.oracle_open()
for i in tqdm(range(len(seoul_df))):
    print(i)
    sql_query2 = (
        f'insert into housePrice(자치구명,법정동명,지번구분명,층수,계약일,전월세,면적,보증금,임대료,건물명,건축년도)'
        f'values({repr(seoul_df.자치구명.iloc[i])},{repr(seoul_df.법정동명.iloc[i])},{repr(seoul_df.지번구분명.iloc[i])},'
        f'{seoul_df.층수.iloc[i]},{seoul_df.계약일.iloc[i]},{repr(seoul_df.전월세.iloc[i])},{seoul_df.면적.iloc[i]},'
        f'{seoul_df.보증금.iloc[i]},{seoul_df.임대료.iloc[i]},{repr(seoul_df.건물명.iloc[i])},{repr(seoul_df.건축년도.iloc[i])})'
    )
    mu.oracle_execute(oracle_query2)
mu.oracle_close()

# MYSQL
sql_conn = mu.connect_mysql('lol_icia')
for i in tqdm(range(len(seoul_df))):
    sql_query2 = (
        f'insert into housePrice(자치구명,법정동명,지번구분명,층수,계약일,전월세,면적,보증금,임대료,건물명,건축년도)'
        f'values({repr(seoul_df.자치구명.iloc[i])},{repr(seoul_df.법정동명.iloc[i])},{repr(seoul_df.지번구분명.iloc[i])},'
        f'{seoul_df.층수.iloc[i]},{seoul_df.계약일.iloc[i]},{repr(seoul_df.전월세.iloc[i])},{seoul_df.면적.iloc[i]},'
        f'{seoul_df.보증금.iloc[i]},{seoul_df.임대료.iloc[i]},{repr(seoul_df.건물명.iloc[i])},{repr(seoul_df.건축년도.iloc[i])})'
    )
    mu.mysql_execute(sql_query2, sql_conn)
sql_conn.commit()
sql_conn.close()

'''
서울시 공공데이터 포탈-서울시 코로나19 확진자 발생동향
api를 통해서 함수 만들었던 것으로 데이터 불러오기
'S_DT','S_HJ','S_CARE','S_RECOVER','S_DEATH','T_HJ','DEATH'
컬럼명 - 서울시기준일, 서울시확진자, 서울시치료중, 서울시퇴원, 서울시사망, 전국확진, 전국사망
DB에 테이블 생성 INSERT(Corona19Countstatus)
서울시기준일 varchar(50), 서울시확진자 number(20), 서울시치료중 number(20),
서울시퇴원 varchar(20), 서울시사망 number(20), 전국확진 number(20), 전국사망 number(20)
'''
# df 생성,컬럼 정리
corona_df = mu.df_creater('http://openapi.seoul.go.kr:8088/(인증키)/xml/TbCorona19CountStatus/1/5/')
sel_d = ['S_DT', 'S_HJ', 'S_CARE', 'S_RECOVER', 'S_DEATH', 'T_HJ', 'DEATH']
seoul_corona_df = corona_df[sel_d]
seoul_corona_df.columns = ['서울시기준일', '서울시확진자', '서울시치료중', '서울시퇴원', '서울시사망', '전국확진', '전국사망']
seoul_corona_df = seoul_corona_df.astype({'서울시확진자': 'int', '서울시치료중': 'int', '서울시사망': 'int', '전국확진': 'int', '전국사망': 'int'})

# ORACLE
oracle_conn = mu.oracle_open()
oracle_query = """
CREATE TABLE Corona19CountStatus(서울시기준일 varchar(50), 서울시확진자 number(20), 서울시치료중 number(20),
서울시퇴원 varchar(20), 서울시사망 number(20), 전국확진 number(20), 전국사망 number(20))
"""
mu.oracle_execute(oracle_query)

for i in tqdm(range(len(seoul_corona_df))):
    oracle_query2 = (
        f'INSERT INTO Corona19CountStatus(서울시기준일, 서울시확진자, 서울시치료중, 서울시퇴원, 서울시사망, 전국확진, 전국사망)'
        f'VALUES ({repr(seoul_corona_df.서울시기준일.iloc[i])}, {seoul_corona_df.서울시확진자.iloc[i]},'
        f'{seoul_corona_df.서울시치료중.iloc[i]}, {repr(seoul_corona_df.서울시퇴원.iloc[i])}, {seoul_corona_df.서울시사망.iloc[i]},'
        f'{seoul_corona_df.전국확진.iloc[i]}, {seoul_corona_df.전국사망.iloc[i]})'
    )
    mu.oracle_execute(oracle_query2)

oracle_query3 = 'select * from Corona19CountStatus'
mu.oracle_execute(oracle_query3)
mu.oracle_close()

# MYSQL
sql_conn = mu.connect_mysql('lol_icia')
sql_query = """
CREATE TABLE Corona19CountStatus(서울시기준일 varchar(50), 서울시확진자 int(20), 서울시치료중 int(20),
서울시퇴원 varchar(20), 서울시사망 int(20), 전국확진 int(20), 전국사망 int(20))
"""
mu.mysql_execute(sql_query, sql_conn)
for i in tqdm(range(len(seoul_corona_df))):
    sql_query2 = (
        f'INSERT INTO Corona19CountStatus(서울시기준일, 서울시확진자, 서울시치료중, 서울시퇴원, 서울시사망, 전국확진, 전국사망)'
        f'VALUES ({repr(seoul_corona_df.서울시기준일.iloc[i])}, {seoul_corona_df.서울시확진자.iloc[i]},'
        f'{seoul_corona_df.서울시치료중.iloc[i]}, {repr(seoul_corona_df.서울시퇴원.iloc[i])}, {seoul_corona_df.서울시사망.iloc[i]},'
        f'{seoul_corona_df.전국확진.iloc[i]}, {seoul_corona_df.전국사망.iloc[i]})'
    )
    mu.mysql_execute(sql_query2, sql_conn)
sql_conn.commit()

sql_query3 = 'select * from Corona19CountStatus'
pd.DataFrame(mu.mysql_execute(sql_query3, sql_conn))
sql_conn.close()

# 자동

# df의 컬럼명 추출


def df_automatic(dff):
    dff_keys = []
    for idx, i in enumerate(dff.keys()):
        dff_keys.append(i)
    return list(dff_keys)


df_automatic(seoul_corona_df)

# create table
test_query = "CREATE TABLE Corona20CountStatus("   # 테이블명,df이름 인자로 지정
for idx, i in enumerate(df_automatic(seoul_corona_df)):  # 컬럼 추출 메소드를 이용 create 쿼리문
    if idx == len(df_automatic(seoul_corona_df))-1:
        test_query += i+' varchar(50))'
    else:
        test_query += i+' varchar(50),'
test_query

sql_conn = mu.connect_mysql('lol_icia')
mu.mysql_execute(test_query, sql_conn)


# insert
first_query = 'INSERT INTO Corona20CountStatus('

for idx, i in enumerate(df_automatic(seoul_corona_df)):
    if idx == len(df_automatic(seoul_corona_df))-1:
        first_query += i+')'
    else:
        first_query += i+', '
first_query

for i in range(len(seoul_corona_df)):  # 컬럼 추출 메소드, iloc를 이용해 각 튜플의 밸류 설정 후 insert
    second_query = ' VALUES ('
    for idx, j in enumerate(df_automatic(seoul_corona_df)):
        if idx == len(df_automatic(seoul_corona_df)) - 1:
            second_query += repr(str(seoul_corona_df[j].iloc[i])) + ")"
            def_test_query = first_query+second_query
            print(def_test_query)
            # mu.mysql_execute(def_test_query, sql_conn)
        else:
            second_query += repr(str(seoul_corona_df[j].iloc[i])) + ', '

sql_conn.commit()
pd.DataFrame(mu.mysql_execute('select * from Corona21CountStatus', sql_conn))
sql_conn.close()

import auto_df
auto_df.df_fullauto(seoul_corona_df, 'Corona21CountStatus')
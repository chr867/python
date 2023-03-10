import cx_Oracle
import pandas as pd
import requests

url = 'https://api.upbit.com/v1/candles/minutes/1'
querystring={'market': "KRW-DOGE", "count":"200"}
headers = {"Accept": "application/json"}
response = requests.request("GET", url, headers=headers, params=querystring)

res=response.json()
doge_df=pd.DataFrame(res)
doge_df

for idx , i in enumerate(doge_df.candle_date_time_kst):
    doge_df.candle_date_time_kst[idx]=i.split('T')[1]

doge_df['diff_price']='none'
doge_df['diff_price']=doge_df['high_price']-doge_df['low_price']

doge_df.candle_date_time_kst

doge_df.candle_date_time_utc = doge_df.candle_date_time_utc.str[11:]

doge_df.candle_date_time_utc = doge_df.candle_date_time_utc.str.replace('2023-03-09T', '')

doge_df.candle_date_time_utc

pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 20)
desired_width = 320
pd.set_option('display.width', desired_width)

url = 'https://api.upbit.com/v1/candles/days'
headers = {'Accept': 'application/json'}
querystring = {'market':"KRW-ETH", "count":"200"}
response = requests.request("GET", url, headers=headers, params=querystring)
response = response.json()
eth_df = pd.DataFrame(response)
eth_df = eth_df[['market', 'candle_date_time_kst', 'opening_price', 'high_price', 'low_price', 'trade_price']].copy()

for idx , i in enumerate(eth_df.candle_date_time_kst):
    eth_df.candle_date_time_kst[idx]=i.split('T')[0]

eth_df.columns

eth_df['m5'] = 'none'
eth_df['m10'] = 'none'

eth_df.set_index('candle_date_time_kst', inplace=True)

eth_df.loc[['2023-01-01']]
eth_df.iloc[[13]]
eth_df[:20]
eth_df[::-1]
eth_df[:6]

eth_df['m5'].iloc[13]=eth_df.iloc[13-5:13].trade_price.mean()
eth_df['m5'].iloc[13]

for idx, i in enumerate(eth_df.m5):
    eth_df['m5'].iloc[idx] = eth_df.iloc[idx-5:idx].trade_price.mean()
    eth_df['m10'].iloc[idx] = eth_df.iloc[idx-10:idx].trade_price.mean()

eth_df['m10'].iloc[14]
eth_df[:15]

eth_df = eth_df.fillna(0)
eth_df.dropana()

eth_df.m5

eth_df

# 서울시 전,월세가 정보
api_key = '77676c6c566368723431555642674c'
url = f'http://openapi.seoul.go.kr:8088/{api_key}/json/tbLnOpendataRentV/1/1000/'
res = requests.get(url).json()
key = list(res.keys())[0]
res[key].keys()
res[key]['row'].keys()
df = pd.DataFrame(res[key]['row'])
df.keys()
sel_c = ['SGG_NM', 'BJDONG_NM', 'LAND_GBN_NM', 'FLR_NO', 'CNTRCT_DE', 'RENT_GBN', 'RENT_AREA', 'RENT_GTN', 'RENT_FEE', 'BLDG_NM', 'BUILD_YEAR']

seoul_df = df[sel_c]
seoul_df.columns=['자치구명', '법정동명', '지번구분명', '층수', '계약일', '전월세', '면적', '보증금', '임대료', '건물명', '건축년도']
seoul_df
# (전월세 = 전세/월세(monthly) - 전세(charter)) 전세만 뽑아서 데이터 프레임 만들기
# astype -> 보증금을 astype으로 int,float으로 변경
# 분리된 데이터프레임으로 자치구명을 기준으로 group by를 해서 보증금,임대료의 평균
# group by된 데이터 프레임을 sort해서 내림차순으로 정렬
# 임대료/보증금 round(4)해서 값 구하기
seoul_df['전월세']
seoul_df.loc[0]
seoul_df

seoul_df_charter = pd.DataFrame()
seoul_df_monthly = pd.DataFrame()

# seoul_df_charter = seoul_df[seoul_df['전월세'] == '전세']
# seoul_df_monthly = seoul_df[seoul_df['전월세'] == '월세']

for idx, i in enumerate(seoul_df['전월세']):
    if i == '전세':
        seoul_df_charter = seoul_df_charter.append(seoul_df.loc[idx])
    else:
        seoul_df_monthly = seoul_df_monthly.append(seoul_df.loc[idx])

seoul_df_monthly
seoul_df_monthly.dtypes

# seoul_df_charter = seoul_df_charter.astype({'보증금':'int', '임대료':'int'})
# seoul_df_monthly = seoul_df_monthly.astype({'보증금':'int', '임대료':'int'})

seoul_df_charter['보증금'] = seoul_df_charter['보증금'].astype('int')
seoul_df_charter['임대료'] = seoul_df_charter['임대료'].astype('int')
seoul_df_monthly['보증금'] = seoul_df_monthly['보증금'].astype('int')
seoul_df_monthly['임대료'] = seoul_df_monthly['임대료'].astype('int')

seoul_df_charter_group = seoul_df_charter.groupby('자치구명')[['보증금', '임대료']].mean().round(2)
seoul_df_monthly_group = seoul_df_monthly.groupby('자치구명')[['보증금', '임대료']].mean().round(2)

seoul_df_monthly_group.sort_values(by=['보증금'], ascending=False)
seoul_df_charter_group.sort_values(by=['보증금'], ascending=False)

seoul_df_monthly['임대료/보증금'] = 'none'

seoul_df_monthly['임대료/보증금'] = (seoul_df_monthly['임대료']/seoul_df_monthly['보증금']).round(4)

result_df = seoul_df_monthly.groupby('자치구명')[['보증금', '임대료', '임대료/보증금']].mean().round(2)
result_df

import cx_Oracle
import pymysql

# 오라클
dsn = cx_Oracle.makedsn('localhost', 1521, 'xe')
db = cx_Oracle.connect(user='ICIA', password='1234', dsn=dsn)
sql_oracle = db.cursor()

# mysql
conn = pymysql.connect(host='localhost', user='root', password='1234', db='lol_icia', charset='utf8')
sql_mysql = conn.cursor()


query = 'create table test2(id number(1), name varchar(5), constraint test113_pk primary key(id))'  # 오라클 쿼리
query3 = 'select * from test2'

query2 = 'create table test_mysql(id smallint, name varchar(5))'  # mysql 쿼리
query4 = 'select * from test_mysql'

# 오라클
sql_oracle.execute(query)
pd.read_sql(sql=query3, con=db)

# mysql
sql_mysql.execute(query2)
pd.read_sql(sql=query4, con=conn)

from datetime import datetime

datetime.today()
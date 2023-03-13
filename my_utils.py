import time

import pandas as pd
import pymysql
import cx_Oracle
import requests

dsn = cx_Oracle.makedsn('localhost', 1521, 'xe')
seoul_api_key = '77676c6c566368723431555642674c'
riot_api_key = 'RGAPI-f44ca538-b547-401d-a6bc-5ee964133c0f'

# Oracle


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


# mysql


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


def match_timeline(summoner_name, num):
    result = []

    def get_puuid(summoner_name_p):
        print(summoner_name_p)
        summoner_get_url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner_name_p}?api_key={riot_api_key}'
        summoner_get_res = requests.get(summoner_get_url).json()
        return get_matches_id(summoner_get_res['puuid'])

    def get_matches_id(puuid):
        get_matches_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?type=ranked&start=0&count={num}&api_key={riot_api_key}'
        get_matches_res = requests.get(get_matches_url).json()
        return get_match_info(get_matches_res)

    def get_match_info(match_ids):
        result_p = []
        for match_id in match_ids:
            get_match_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={riot_api_key}'
            get_match_res = requests.get(get_match_url).json()
            get_timeline_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline?api_key={riot_api_key}'
            get_timeline_res = requests.get(get_timeline_url).json()
            result_p.append({'match_id': match_id, 'match': get_match_res, 'timeline': get_timeline_res})
        return result_p

    for n in summoner_name:
        try:
            result.append(get_puuid(n))
            time.sleep(1)
        except:
            continue
    res_list = []
    for i in result:
        for j in i:
            res_list.append([j['match_id'], j['match'], j['timeline']])
    return res_list


# def get_puuid(user):
#     url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/{user}?api_key={riot_api_key}'
#     res = requests.get(url).json()
#     puuid = res['puuid']
#     return puuid
#
#
# def get_match_id(puuid, num):
#     url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?type=ranked&start=0&count={num}&api_key={riot_api_key}'
#     res = requests.get(url).json()
#     return res
#
#
# def get_matches_timelines(match_id):
#     url1 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={riot_api_key}'
#     res1 = requests.get(url1).json()
#     url2 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline?api_key={riot_api_key}'
#     res2 = requests.get(url2).json()
#     return res1, res2



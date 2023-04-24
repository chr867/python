import datetime

import pandas as pd
import requests
from tqdm import tqdm
import private
import my_utils as mu
import json
import pymysql
tqdm.pandas()

import imp
imp.reload(private)
'''
전적 페이지로 이동(소환사 이름)
소환사 이름으로 db 조회
flask에서 정제한 df(횟수, 승률, KDA)를 json으로 변환해서 return
'''

riot_api_keys = private.riot_api_key_array

# 소환사 이름 불러오기
def load_summoner_names():
    tiers = ['C', 'GM', 'M']
    # divisions = ['I', 'II', 'III', 'IV']
    name_lst = []
    for tier in tqdm(tiers):
            page_p = 1
            _it = iter(riot_api_keys)
            while True:
                try:
                    if tier == 'C':
                        url_p = f'https://kr.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5?api_key={next(_it)}'
                        res_p = requests.get(url_p).json()

                    elif tier == 'GM':
                        url_p = f'https://kr.api.riotgames.com/lol/league/v4/grandmasterleagues/by-queue/RANKED_SOLO_5x5?api_key={next(_it)}'
                        res_p = requests.get(url_p).json()

                    elif tier == 'M':
                        url_p = f'https://kr.api.riotgames.com/lol/league/v4/masterleagues/by-queue/RANKED_SOLO_5x5?api_key={mu.riot_api_key}'
                        res_p = requests.get(url_p).json()

                except StopIteration:
                    _it = iter(riot_api_keys)

                except Exception as e:
                    print(e)
                    continue
                for summoner in res_p:
                    name_lst.append(summoner['summonerName'])
                if len(res_p) < 200:
                    break
                page_p += 1
    return set(name_lst)
# 소환사 이름 불러오기 끝

# 소환사 이름으로 match_id 불러오기
def load_match_ids(name_set):
    index = 0
    match_ids = []
    for summoner_name in tqdm(name_set):
        api_key = iter(riot_api_keys)
        try:
            url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner_name}?api_key={next(api_key)}'
        except:
            api_key = iter(riot_api_keys)
        summoner_names = iter(name_set)
        res = requests.get(url).json()
        puuid = res['puuid']
        url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?startTime=1673485200&type=ranked&start={index}&count=100&api_key={i}'
        res = requests.get(url).json()
        index += 100
        match_ids.extend(res)
        if len(res) < 10:
            index = 0
            continue
    sett = set(match_ids)
# match_id 끗

# match_id, matches, timeline 폼으로 만들기
def get_match_info(_match_ids):
    print('get_match_info')
    _result = []
    _it = iter(riot_api_keys)
    for match_id in tqdm(_match_ids):
        try:
            _riot_api_key = next(_it)
        except StopIteration:
            _it = iter(riot_api_keys)
            continue
        get_match_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={_riot_api_key}'
        get_match_res = requests.get(get_match_url).json()
        get_timeline_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline?api_key={_riot_api_key}'
        get_timeline_res = requests.get(get_timeline_url).json()
        _result.append([match_id, get_match_res, get_timeline_res])
    return pd.DataFrame(_result, columns=['match_id', 'matches', 'timeline'])
# 끝


# insert
def insert(t, conn):
    matches_json, timeline_json = conn.escape_string(json.dumps(t.matches)), conn.escape_string(json.dumps(t.timeline))
    sql_insert = (
        f"insert ignore into newtable (match_id, matches, timeline) values ({repr(t.match_id)}, '{matches_json}', '{timeline_json}') "
    )
    mu.mysql_execute(sql_insert, conn)
    conn.commit()

# sql_conn = mu.connect_mysql('lol_icia')
# result.progress_apply(lambda x: insert(x, sql_conn), axis=1)
# sql_conn.close()
# insert 끝


# 소환사 이름 반복
def matches_timeline(_name_lst_set):
    for summoner_name in tqdm(_name_lst_set[224:]):
        match_ids = []
        index = 0
        start = 1673485200  # 시즌 시작 Timestamp
        run_time = 1681894800  # 코드 돌린 Timestamp
        api_key = iter(riot_api_keys)

        try:
            try:
                i = next(api_key)
                url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner_name}?api_key={i}'
            except:
                api_key = iter(riot_api_keys)

            res = requests.get(url).json()
            puuid = res['puuid']

            while True:
                url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?startTime={start}&type=ranked&start={index}&count=100&api_key={i}'
                res = requests.get(url).json()
                index += 100
                match_ids.extend(res)

                if len(res) < 10:
                    break
        except:
            continue

        sett = set(match_ids)
        result = get_match_info(sett)
        sql_conn = mu.connect_mysql('lol_icia')
        result.progress_apply(lambda x: insert(x, sql_conn), axis=1)
        sql_conn.close()
# 끝


name_set = load_summoner_names()
name_lst = list(name_set)

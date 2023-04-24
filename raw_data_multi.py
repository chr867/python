import datetime
import time

import pandas as pd
import requests
from tqdm import tqdm
import private
import my_utils as mu
import json
tqdm.pandas()

'''
전적 페이지로 이동(소환사 이름)
소환사 이름으로 db 조회
flask에서 정제한 df(횟수, 승률, KDA)를 json으로 변환해서 return
'''

riot_api_keys = private.riot_api_key_array

# 소환사 이름 불러오기
def load_summoner_names():
    print('load_summoner_names...')
    tiers = ['IRON', 'BRONZE', 'SILVER', 'GOLD', 'PLATINUM', 'DIAMOND']
    divisions = ['I', 'II', 'III', 'IV']
    name_lst = []
    for tier in tqdm(tiers):
        for division in tqdm(divisions):
            page_p = 1
            _it = iter(riot_api_keys)
            while True:
                try:
                    url_p = f'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page_p}&api_key={next(_it)}'
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
    print('load_summoner_names 종료', len(name_lst))
    return set(name_lst)
# 소환사 이름 불러오기 끝


# 소환사 이름으로 match_id 불러오기
def load_match_ids(name_set):
    print('load_match_ids...')
    index = 0
    match_ids = []
    for summoner_name in tqdm(name_set):
        api_key = iter(riot_api_keys)
        try:
            url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner_name}?api_key={next(api_key)}'
        except StopIteration:
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
    print('load_match_ids 종료')
    return set(match_ids)
# match_id 끗


# match_id, matches, timeline 폼으로 만들기
def get_match_info(_match_ids):
    print('get_match_info....')
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
    print('get_match_info 종료')
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
    for summoner_name in tqdm(_name_lst_set):
        match_ids = []
        index = 0
        start = 1673485200  # 시즌 시작 Timestamp
        run_time = 1681894800  # 코드 돌린 Timestamp
        api_key = iter(riot_api_keys)

        try:
            try:
                i = next(api_key)
                url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner_name}?api_key={i}'
            except StopIteration:
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
        except Exception as e:
            print(e)
            continue

        sett = set(match_ids)
        result = get_match_info(sett)
        sql_conn = mu.connect_mysql('lol_icia')
        result.progress_apply(lambda x: insert(x, sql_conn), axis=1)
        sql_conn.close()
# 끝

import multiprocessing as mp

# 멀티 프로세싱
def main():
    # Load summoner names using 4 processes
    with mp.Pool(processes=4) as pool:
        name_set = set()
        for result in pool.map(load_summoner_names_worker, [0, 1, 2, 3]):
            name_set.update(result)

    # Fetch matches timeline using 4 processes
    with mp.Pool(processes=4) as pool:
        chunk_size = len(name_set) # 4  # 청크 크기 수정
        chunks = [name_set[i:i + chunk_size] for i in range(0, len(name_set), chunk_size)]

        results = [pool.apply_async(matches_timeline_worker, args=(chunk,)) for chunk in chunks]
        output = [p.get() for p in results]


def load_summoner_names_worker(worker_id):
    # Divide the work among the workers
    tiers = ['IRON', 'BRONZE', 'SILVER', 'GOLD', 'PLATINUM', 'DIAMOND']
    divisions = ['I', 'II', 'III', 'IV']
    name_lst = []
    for i in range(worker_id, len(tiers) * len(divisions), 4):
        tier = tiers[i // len(divisions)]
        division = divisions[i % len(divisions)]
        page_p = 1
        _it = iter(riot_api_keys)
        while True:
            try:
                try:
                    url_p = f'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page_p}&api_key={next(_it)}'
                    res_p = requests.get(url_p).json()
                except StopIteration:
                    _it = iter(riot_api_keys)
                for summoner in res_p:
                    name_lst.append(summoner['summonerName'])
            except Exception as e:
                print(e)
                continue
            if len(res_p) < 200:
                break
            page_p += 1
            time.sleep(0.05)
    return set(name_lst)


def matches_timeline_worker(name_set):
    # Fetch matches timeline for a set of summoner names
    matches_timeline(name_set)


if __name__ == '__main__':
    main()

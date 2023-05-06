import datetime
import random
import time
import pandas as pd
import requests
from tqdm import tqdm
import private
import my_utils as mu
import json
import multiprocessing as mp
tqdm.pandas()

'''
전적 페이지로 이동(소환사 이름)
소환사 이름으로 db 조회
flask에서 정제한 df(횟수, 승률, KDA)를 json으로 변환해서 return
'''

riot_api_keys = private.riot_api_key_array

def load_summoner_names_worker(worker_id):
    api_key = riot_api_keys[worker_id]
    tier_division = [
        [['GOLD', 'IV'], ['IRON', 'IV'], ['IRON', 'III']],
        [['SILVER', 'IV'], ['IRON', 'II'], ['PLATINUM', 'II']],
        [['SILVER', 'II'], ['IRON', 'I'], ['PLATINUM', 'III']],
        [['SILVER', 'III'], ['GOLD', 'I'], ['DIAMOND', 'IV']],
        [['SILVER', 'I'], ['GOLD', 'II'], ['DIAMOND', 'I']],
        [['BRONZE', 'II'], ['BRONZE', 'III'], ['DIAMOND', 'III']],
        [['GOLD', 'III'], ['BRONZE', 'I'], ['DIAMOND', 'II']],
        [['BRONZE', 'IV'], ['PLATINUM', 'IV'], ['PLATINUM', 'I']]
    ]
    for i in tqdm(tier_division[worker_id]):
        tier = i[0]
        division = i[1]
        page_p = 1
        name_lst = []

        while True:
            try:
                url = f'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page_p}&api_key={api_key}'
                res_p = requests.get(url).json()

                for summoner in res_p:
                    name_lst.append(summoner['summonerId'])

            except Exception as e:
                print(f'{e} 예외 발생, {res_p["status"]["message"]}, {api_key}')
                time.sleep(10)
                continue

            if len(res_p) < 200:
                break
            if len(name_lst) > 5000:
                break
            page_p += 1

        match_ids = set()
        for summoner_name in tqdm(name_lst):
            while True:
                index = 0
                start = 1673485200  # 시즌 시작 Timestamp
                run_time = int(time.time() * 1000)  # 코드 돌린 Timestamp
                try:

                    url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/{summoner_name}?api_key={api_key}'
                    res = requests.get(url).json()
                    puuid = res['puuid']
                    # time.sleep(1.3)

                    while True:
                        url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?startTime={start}&type=ranked&start={index}&count=100&api_key={api_key}'
                        res = requests.get(url).json()
                        index += 100
                        match_ids.update(res)
                        if len(res) < 10:
                            break
                        # time.sleep(1.3)

                except Exception as e:
                    if 'found' in res['status']['message']:
                        print(summoner_name, 'not found')
                        break

                    print(f'{e} 예외 발생, {res["status"]["message"]},{api_key}')
                    time.sleep(10)
                    continue

                break
    match_ids = list(match_ids)
    print('load_summoner_names END', worker_id, len(match_ids))
    return match_ids

# matches_timeline
def matches_timeline(_name_lst, i):
    match_ids = set()
    api_key = riot_api_keys[i]

    for summoner_name in tqdm(_name_lst):
        while True:
            index = 0
            start = 1673485200  # 시즌 시작 Timestamp
            run_time = int(time.time() * 1000)  # 코드 돌린 Timestamp
            try:

                url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/{summoner_name}?api_key={api_key}'
                res = requests.get(url).json()
                print(res)
                puuid = res['puuid']

                while True:
                    url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?startTime={start}&type=ranked&start={index}&count=100&api_key={api_key}'
                    res = requests.get(url).json()
                    index += 100
                    match_ids.update(res)
                    if len(res) < 10:
                        break

            except Exception as e:
                if 'found' in res['status']['message']:
                    print(summoner_name, 'not found')
                    break

                print(f'{e} 예외 발생, {res["status"]["message"]}')
                time.sleep(10)
                continue

            break
    return list(match_ids)
# 끝

# match_id, matches, timeline 폼으로 만들기
def get_match_info(_match_ids, i):
    _result = []
    api_key = riot_api_keys[i]

    for match_id in tqdm(_match_ids):
        while True:
            try:
                get_match_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={api_key}'
                get_match_res = requests.get(get_match_url).json()

                if 'status' in get_match_res:
                    raise Exception(f'get match {get_match_res["status"]["message"]}')

                get_timeline_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline?api_key={api_key}'
                get_timeline_res = requests.get(get_timeline_url).json()

                if 'status' in get_timeline_res:
                    raise Exception(f'get timeline {get_timeline_res["status"]["message"]},{api_key}')

            except Exception as e:

                print(f'{e} 예외 발생 {api_key}')
                time.sleep(20)
                continue

            _result.append([match_id, get_match_res, get_timeline_res])
            break

    result_df = pd.DataFrame(_result, columns=['match_id', 'matches', 'timeline'])
    print('get_match_info END len = ', len(result_df))
    return result_df
# 끝

# insert
def insert(t, conn):
    matches_json, timeline_json = conn.escape_string(json.dumps(t.matches)), conn.escape_string(json.dumps(t.timeline))
    sql_insert = (
        f"insert ignore into match_raw (match_id, matches, timeline) values ({repr(t.match_id)}, '{matches_json}', "
        f"'{timeline_json}')"
    )
    mu.mysql_execute(sql_insert, conn)
# insert 끝


def matches_timeline_worker(args):
    name_lst, i = args
    return matches_timeline(name_lst, i)

def get_match_info_worker(args):
    match_id, i = args
    match_info = get_match_info(match_id, i)
    return match_info

conn = mu.connect_mysql('my_db')
def insert_worker(match_info):
    global conn
    match_info.apply(lambda x: insert(x, conn), axis=1)

def main():
    # Load summoner names using 8 processes
    with mp.Pool(processes=8) as pool:
        print('**load_summoner_names**  get_match_info  matches_timeline')
        match_ids = set()
        for i, result in enumerate(tqdm(pool.imap(load_summoner_names_worker, range(8)))):
            match_ids.update(result)

    match_ids = list(match_ids)
    print('load_summoner_names  matches_timeline  **get_match_info**', len(match_ids))
    with mp.Pool(processes=8) as pool:
        chunk_size = len(match_ids)
        chunks = [match_ids[i:i + chunk_size // 8] for i in range(0, len(match_ids), chunk_size // 8)]
        match_info_output = []
        for i, res in enumerate(tqdm(pool.imap(get_match_info_worker, zip(chunks, range(8))), total=len(chunks))):
            match_info_output.append(res)

    merged_df = pd.concat(match_info_output)
    print("merged_df =", len(merged_df))
    n_rows = len(merged_df) // 12  # 하나의 부분 데이터프레임에 들어갈 행의 수
    df_chunks = [merged_df[i:i + n_rows] for i in range(0, len(merged_df), n_rows)]
    print("df_chunks = ", len(df_chunks))
    with mp.Pool(processes=12) as pool:
        for df_chunk in df_chunks:
            chunk_size2 = len(df_chunk)
            chunk_step = chunk_size2 // 12
            chunks2 = [df_chunk[i:i + chunk_step] for i in range(0, chunk_size2, chunk_step)]

            for i in tqdm(pool.imap(insert_worker, chunks2), total=len(chunks2)):
                pass

if __name__ == '__main__':
    main()
    conn.commit()
    conn.close()

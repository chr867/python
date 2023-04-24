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

import multiprocessing as mp

# 멀티 프로세싱
# def main():
#     # Load summoner names using 4 processes
#     with mp.Pool(processes=4) as pool:
#         name_set = set()
#         for result in tqdm(pool.map(load_summoner_names_worker, [0, 1, 2, 3])):
#             name_set.update(result)
#             time.sleep(1)
#     # Fetch matches timeline using 4 processes
#     with mp.Pool(processes=4) as pool:
#         chunk_size = len(name_set) # 4  # 청크 크기 수정
#         chunks = [name_set[i:i + chunk_size] for i in range(0, len(name_set), chunk_size)]
#
#         results = [pool.apply_async(matches_timeline_worker, args=(chunk,)) for chunk in chunks]
#         output = [p.get() for p in results]
#         time.sleep(1)

def load_summoner_names_worker(worker_id):
    # Divide the work among the workers
    print('**load_summoner_names**  get_match_info  matches_timeline')
    time.sleep(1)

    tiers = ['IRON', 'BRONZE', 'SILVER', 'GOLD', 'PLATINUM', 'DIAMOND']
    divisions = ['I', 'II', 'III', 'IV']
    name_lst = []
    for i in range(worker_id, len(tiers) * len(divisions), 4):
        tier = tiers[i // len(divisions)]
        division = divisions[i % len(divisions)]
        page_p = 1
        api_it = iter(riot_api_keys)
        # for k in range(2):
        while True:
            try:
                try:
                    api_key = next(api_it)
                except StopIteration:
                    api_it = iter(riot_api_keys)
                    api_key = next(api_it)

                url_p = f'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page_p}&api_key={api_key}'
                res_p = requests.get(url_p).json()

                for summoner in res_p:
                    name_lst.append(summoner['summonerName'])

            except Exception as e:
                print(f'{e} 예외 발생')
                continue

            if len(res_p) < 200:
                break

            page_p += 1
            print('worker = ', worker_id, 'len = ', len(name_lst), ' api_key = ', riot_api_keys.index(api_key))
            time.sleep(0.5)
    print('load_summoner_names END')
    return list(set(name_lst))

# match_id, matches, timeline 폼으로 만들기
def get_match_info(_match_ids):
    print('load_summoner_names  matches_timeline  **get_match_info**')
    _result = []
    api_it = iter(riot_api_keys)
    for match_id in _match_ids:
        try:
            api_key = next(api_it)
        except StopIteration:
            api_it = iter(riot_api_keys)
            api_key = next(api_it)

        try:
            get_match_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={api_key}'
            get_match_res = requests.get(get_match_url).json()
            time.sleep(1)
            get_timeline_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline?api_key={api_key}'
            get_timeline_res = requests.get(get_timeline_url).json()
            _result.append([match_id, get_match_res, get_timeline_res])
        except Exception as e:
            print(f'{e} 예외 발생')
            continue
    result_df = pd.DataFrame(_result, columns=['match_id', 'matches', 'timeline'])
    print('get_match_info END len = ', len(result_df))
    return result_df
# 끝

# insert
def insert(t, conn):

    if 'status' in t.matches[:10]:
        print(f'{t.match_id} rate limit')
        return

    matches_json, timeline_json = conn.escape_string(json.dumps(t.matches)), conn.escape_string(json.dumps(t.timeline))
    sql_insert = (
        f"insert ignore into newtable (match_id, matches, timeline) values ({repr(t.match_id)}, '{matches_json}', '{timeline_json}') "
    )
    mu.mysql_execute(sql_insert, conn)
    conn.commit()
# insert 끝

# 소환사 이름 반복
def matches_timeline(_name_lst_set):
    print('load_summoner_names  **matches_timeline**  get_match_info')
    for summoner_name in tqdm(_name_lst_set):
        match_ids = []
        index = 0
        start = 1673485200  # 시즌 시작 Timestamp
        run_time = int(time.time() * 1000)  # 코드 돌린 Timestamp
        api_it = iter(riot_api_keys)

        try:
            try:
                api_key = next(api_it)
            except StopIteration:
                api_it = iter(riot_api_keys)
                api_key = next(api_it)

            url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner_name}?api_key={api_key}'
            res = requests.get(url).json()
            puuid = res['puuid']

            while True:
                url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?startTime={start}&type=ranked&start={index}&count=100&api_key={api_key}'
                res = requests.get(url).json()
                index += 100
                match_ids.extend(res)
                if len(res) < 10:
                    break
        except Exception as e:
            print(f'{e} 예외 발생')
            continue

        tmp_list = list(set(match_ids))

        result = get_match_info(tmp_list)

        sql_conn = mu.connect_mysql('lol_icia')
        result.apply(lambda x: insert(x, sql_conn), axis=1)
        sql_conn.close()
    print('matches_timeline END')
# 끝

def matches_timeline_worker(name_set):
    # Fetch matches timeline for a set of summoner names
    matches_timeline(name_set)

# 진행 상황 체크 버전
# def main():
#     # Load summoner names using 4 processes
#     with mp.Pool(processes=4) as pool:
#         name_set = []
#         for result in tqdm(pool.map(load_summoner_names_worker, [0, 1, 2, 3])):
#             name_set.extend(result)
#             time.sleep(3)
#     # Fetch matches timeline using 4 processes
#     with mp.Pool(processes=4) as pool:
#         chunk_size = len(name_set) // 4  # 청크 크기 수정
#         chunks = [name_set[i:i + chunk_size] for i in range(0, len(name_set), chunk_size)]
#
#         # 각 프로세스가 처리한 작업의 개수를 저장할 리스트 생성
#         result_count = [0] * len(chunks)
#
#         # imap으로 대체
#         output = []
#         for res in tqdm(pool.imap(matches_timeline_worker, chunks), total=len(chunks)):
#             output.append(res)
#             result_count[output.index(res)] += 1
#             time.sleep(3)
#         # 각 프로세스가 처리한 작업의 개수 합산
#         total_results = sum(result_count)
#         print(f"Total results: {total_results}")

# 지연 버전
def main():
    # Load summoner names using 4 processes
    with mp.Pool(processes=4) as pool:
        name_set = []
        for i, result in enumerate(tqdm(pool.map(load_summoner_names_worker, [0, 1, 2, 3]))):
            name_set.extend(result)
            if i != 0:  # 첫 두 프로세스는 딜레이 없음
                time.sleep(2)  # 2초 딜레이

    # Fetch matches timeline using 4 processes
    with mp.Pool(processes=4) as pool:
        chunk_size = len(name_set) // 4  # 청크 크기 수정
        chunks = [name_set[i:i + chunk_size] for i in range(0, len(name_set), chunk_size)]

        # 각 프로세스가 처리한 작업의 개수를 저장할 리스트 생성
        result_count = [0] * len(chunks)

        # imap으로 대체
        output = []
        for i, res in enumerate(tqdm(pool.imap(matches_timeline_worker, chunks), total=len(chunks))):
            output.append(res)
            result_count[output.index(res)] += 1
            if i != 0:  # 첫 두 프로세스는 딜레이 없음
               time.sleep(2)  # 2초 딜레이

        # 각 프로세스가 처리한 작업의 개수 합산
        total_results = sum(result_count)
        print(f"Total results: {total_results}")

if __name__ == '__main__':
    main()





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
import logging
tqdm.pandas()

riot_api_keys = private.riot_api_key_array

# 티어별 유저 이름, 이름으로 puuid, puuid로 match id
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
        page_p = random.randrange(1, 50)
        name_set = set()

        while True:
            try:
                url = f'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page_p}&api_key={api_key}'
                res_p = requests.get(url).json()

                for summoner in res_p:
                    name_set.add(summoner['summonerId'])

            except Exception:
                if 'Forbidden' in res_p['status']['message']:
                    break

                print(f'suummoner names 예외 발생 {res_p["status"]["message"]}, {api_key}')
                time.sleep(20)
                continue
            print(len(name_set))
            if len(res_p) < 200:
                break

            break

        print('load_summoner_names END', worker_id, len(name_set))
        name_lst = list(name_set)
        random.shuffle(name_lst)

        match_set = set()
        for summoner_name in tqdm(name_lst[:40]):
            while True:
                index = 0
                start = 1673485200  # 시즌 시작 Timestamp
                # tmp = 1683438967290
                try:

                    url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/{summoner_name}?api_key={api_key}'
                    res = requests.get(url).json()
                    puuid = res['puuid']

                    while True:
                        url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?startTime={start}&type=ranked&start={index}&count=100&api_key={api_key}'
                        res = requests.get(url).json()
                        index += 100
                        match_set.update(res)
                        if len(res) < 10:
                            break

                except Exception as e:
                    if 'found' in res['status']['message']:
                        print(summoner_name, 'not found')
                        break

                    print(f'{e} 예외 발생, {res["status"]["message"]},{api_key}')
                    time.sleep(20)
                    continue

                break

    match_list = list(match_set)
    print('load_match_ids END', worker_id, len(match_list))
    return match_list
# 끝

# match_id, matches, timeline 폼으로 만들기
def get_match_info_worker(args):
    _match_ids, i = args
    _result = []
    api_key = riot_api_keys[i]
    tmp = set()
    random.shuffle(_match_ids)

    for match_id in tqdm(_match_ids[:500]):  # 수정점
        while True:
            time.sleep(1.25)
            try:
                get_match_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={api_key}'
                get_match_res = requests.get(get_match_url).json()
                tmp.update(get_match_res['metadata'])
            except Exception as e:
                print(f'{e} match, {get_match_res["status"]["message"]},{api_key}')
                if 'found' in get_match_res['status']['message']:
                    break
                if 'Forbidden' in get_match_res['status']['message']:
                    break
                time.sleep(20)
                continue

            time.sleep(1.25)
            try:
                get_timeline_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline?api_key={api_key}'
                get_timeline_res = requests.get(get_timeline_url).json()
                tmp.update(get_timeline_res['metadata'])
            except Exception as e:
                print(f'{e} timeline, {get_timeline_res},{api_key}')
                if 'found' in get_timeline_res['status']['message']:
                    break
                if 'Forbidden' in get_timeline_res['status']['message']:
                    break
                time.sleep(20)
                continue

            _result.append([match_id, get_match_res, get_timeline_res])
            tmp.clear()
            break

    result_df = pd.DataFrame(_result, columns=['match_id', 'matches', 'timeline'])
    print('get_match_info END len = ', len(result_df))
    return result_df
# 끝

def insert_worker(args):
    match_info, i = args
    time.sleep(i*0.1)
    conn = mu.connect_mysql('my_db')
    match_info.progress_apply(lambda x: insert(x, conn), axis=1)
    conn.commit()
    conn.close()

# insert
def insert(t, conn_):
    try:
        matches_json, timeline_json = conn_.escape_string(json.dumps(t.matches)), conn_.escape_string(json.dumps(t.timeline))
        sql_insert = (
            f"insert ignore into match_raw (match_id, matches, timeline) values ({repr(t.match_id)}, '{matches_json}', "
            f"'{timeline_json}')"
        )
        mu.mysql_execute(sql_insert, conn_)
    except Exception as e:
        logging.exception(f"Error occurred during insert: {e}, {t.match_id}")
# insert 끝

def main():
    run_time = int(time.time() * 1000)  # 코드 돌린 Timestamp
    now = datetime.datetime.now()
    formatted_time = now.strftime("%Y-%m-%d %H:%M")
    file_path = "C:/icia/python/runtime.txt"  # 메모장에 저장할 파일 경로와 이름 설정 헤응
    with open(file_path, "w") as f:  # 파일 열기
        f.write(formatted_time + " " + str(run_time))  # 런타임 값을 파일에 쓰기
    f.close()  # 파일 닫기

    match_ids = set()
    with mp.Pool(processes=8) as pool:
        print('**load_summoner_names**  matches_timeline')
        for i, result in enumerate(tqdm(pool.imap(load_summoner_names_worker, range(8)))):
            match_ids.update(result)

    match_ids = list(match_ids)
    print('load_summoner_names  **get_match_info**', len(match_ids))
    match_info_output = []
    with mp.Pool(processes=8) as pool:
        chunk_size = len(match_ids)
        chunks = [match_ids[i:i + chunk_size // 8] for i in range(0, len(match_ids), chunk_size // 8)]
        for i, res in enumerate(tqdm(pool.imap(get_match_info_worker, zip(chunks, range(8))), total=len(chunks))):
            match_info_output.append(res)

    merged_df = pd.concat(match_info_output)
    print("merged_df =", len(merged_df))

    sql_conn = mu.connect_mysql('my_db')
    merged_df.progress_apply(lambda x: insert(x, sql_conn), axis=1)
    sql_conn.commit()
    sql_conn.close()

    # with mp.Pool(processes=12) as pool:
    #     print('**insert**')
    #     chunk_size = len(merged_df) // 12
    #     chunks2 = [merged_df[i:i + chunk_size] for i in range(0, len(merged_df), chunk_size)]
    #     for _ in tqdm(pool.imap(insert_worker, zip(chunks2, range(12))), total=len(chunks2)):
    #         pass

    print('done')

if __name__ == '__main__':
    for _ in tqdm(range(24)):
        main()
        print('sleep 20')
        time.sleep(20)



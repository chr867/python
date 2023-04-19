import pandas as pd
import requests
from tqdm import tqdm
import private

'''
전적 페이지로 이동(소환사 이름)
소환사 이름으로 db 조회
flask에서 정제한 df(횟수, 승률, KDA)를 json으로 변환해서 return
'''

riot_api_key = private.riot_api_key_array

SN = '댕청잇'
start = 0
match_ids = []
while True:
    for i in tqdm(riot_api_key):
        url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/{SN}?api_key={i}'
        res = requests.get(url).json()
        puuid = res['puuid']
        url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?startTime=1673485200&type=ranked&start={start}&count=100&api_key={i}'
        res = requests.get(url).json()
        start += 100
        match_ids += res
    if len(res) < 10:
        start = 0
        break
sett = set(match_ids)

def get_match_info(_match_ids):
    print('get_match_info')
    _result = []
    _it = iter(riot_api_key)
    for match_id in tqdm(_match_ids):
        try:
            _riot_api_key = next(_it)
        except StopIteration:
            _it = iter(riot_api_key)
            continue
        get_match_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={_riot_api_key}'
        get_match_res = requests.get(get_match_url).json()
        get_timeline_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline?api_key={_riot_api_key}'
        get_timeline_res = requests.get(get_timeline_url).json()
        _result.append([match_id, get_match_res, get_timeline_res])
    return pd.DataFrame(_result)

result = get_match_info(sett)
result2 = get_match_info(sett)
result_df = pd.DataFrame(result)
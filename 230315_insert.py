import requests
import pandas as pd
import my_utils as mu
from tqdm import tqdm
import time
import random

# 티어 + 디비전 ex) 실버1,골드4
url = 'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/DIAMOND/IV?page=1&api_key=RGAPI-2376074f-8336-4e6a-99cf-a0f2fe4b59da'
res = requests.get(url).json()

res[0]['summonerName']
summoner_lst = list(map(lambda x: x['summonerName'], res))

summoner_lst[:10]

lst = []
lst = mu.match_timeline(summoner_lst[:10], 5)
df = pd.DataFrame(lst, columns=['match_id', 'matches', 'timeline'])

# matchid, matches, timeline 으로 구성된 원시데이터 df만드는 함수
# (인자값으로 티어만 넣으면 해당 티어의 디비전 1,2,3,4 사람들 중 랜덤으로 가져오는 함수)


# import random
division_list = ['I', 'II', 'III', 'IV']
tier = 'BRONZE'
riot_api_key = mu.riot_api_key

lst = []
for division in division_list:
    page = random.randrange(1, 10)
    url = f'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page}&api_key={riot_api_key}'
    res = requests.get(url).json()
    lst += random.sample(res, 5)

lst
lst[0]['summonerName']

for idx, i in lst:
    print(idx, i['summonerName'])

# get_rawdata(tier) 함수를 완성하기
# division 리스트와 page를 랜덤으로 뽑아올 함수를 사용하기 lst 리스트도 만들어두기
# riot api를 통해서 summonerName을 가져오기
# summonerName을 통해서 puuid 가져오기
# match_ids 가져오기
# match,timeline rawdata -> (match_id, matches, timeline) df만들기
#  return df


def get_rawdata(tier_p):
    division_list_p = ['I', 'II', 'III', 'IV']
    lst_p = []
    for division_p in division_list_p:
        page_p = random.randrange(1, 10)
        url_p = f'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier_p}/{division_p}?page={page_p}&api_key={mu.riot_api_key}'
        res_p = requests.get(url_p).json()
        lst_p += random.sample(res_p, 5)

    name_lst = [i['summonerName'] for i in lst_p]
    # name_lst = list(map(lambda x: x['summonerName'], lst_p))
    result_res = mu.match_timeline(name_lst, 3)
    result_df = pd.DataFrame(result_res, columns=['match_id', 'matches', 'timeline'])
    return result_df

import imp
imp.reload(mu)

rawdata_df = get_rawdata('SILVER')
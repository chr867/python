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
    print('get_rawdata complete')
    return result_df

import imp
imp.reload(mu)

rawdata_df = get_rawdata('SILVER')

# 원시데이터인 df를 넣어서 ouput match,timeline 데이터가 있는 df를 만들기
# return -> df
# 함수 결과값(df)를 넣을 수 있는 테이블 생성, insert문까지
# pk - (match_id, participantId)
def get_match_timeline_df(df_p):
    # df를 한개로 만들기
    df_creater = []
    columns = ['gameId', 'gameDuration', 'gameVersion', 'summonerName', 'summonerLevel', 'participantId',
               'championName', 'champExperience',
               'teamPosition', 'teamId', 'win', 'kills', 'deaths', 'assists', 'totalDamageDealtToChampions',
               'totalDamageTaken', 'g_5', 'g_6', 'g_7', 'g_8', 'g_9', 'g_10', 'g_11', 'g_12', 'g_13', 'g_14', 'g_15',
               'g_16', 'g_17',
               'g_18', 'g_19', 'g_20', 'g_21', 'g_22', 'g_23', 'g_24', 'g_25']
    print('소환사 스텟 생성중')
    for i in tqdm(range(len(df))):
        # matches 관련된 데이터

        # timeline 관련 데이터
    return df

df_creater = []
rawdata_df['matches'][0]['metadata']['matchId']
# match - ['match_id' 인포,'gameDuration' 인포,'gameVersion' 인포,'summonerName' 인포>파티,'summonerLevel'인포->파티,'participantId'인포->파티
# , 'championName' 인포->파티, 'champExperience' 인포->파티,'teamPosition' 인포->파티, 'teamId' 인포->파티, 'win' 인포->파티,
# 'kills' 인포->파티, 'deaths' 인포->파티 ,'assists' 인포->파티, 'totalDamageDealtToChampions' 인포->파티, 'totalDamageTaken 인포->파티']
# timeline - ['participantId','g5','g6' ~ 'g25'] -> 25분까지 안가는 게임 try except: 0 insert
columns = ['gameId', 'gameDuration', 'gameVersion', 'summonerName', 'summonerLevel', 'participantId',
           'championName', 'champExperience',
           'teamPosition', 'teamId', 'win', 'kills', 'deaths', 'assists', 'totalDamageDealtToChampions',
           'totalDamageTaken', 'g_5', 'g_6', 'g_7', 'g_8', 'g_9', 'g_10', 'g_11', 'g_12', 'g_13', 'g_14', 'g_15',
           'g_16', 'g_17',
           'g_18', 'g_19', 'g_20', 'g_21', 'g_22', 'g_23', 'g_24', 'g_25']

df_creater = []
for idx,i in tqdm(enumerate(rawdata_df['matches'])):
    for idx_p, j in enumerate(i['info']['participants']):
        df_creater.append([
            i['info']['gameId'], i['info']['gameDuration'], i['info']['gameVersion'],
            j['summonerName'], j['summonerLevel'], j['participantId'], j['championName'],
            j['champExperience'], j['teamPosition'], j['teamId'], j['win'], j['kills'],
            j['deaths'], j['assists'], j['totalDamageDealtToChampions'], j['totalDamageTaken'],
        ])
len(df_creater)
for x in range(len(df_creater)):
    print(df_creater[x][0])
    for k in range(5,26):
        try:
            df_creater[x].append(rawdata_df.iloc[x]['timeline']['info']['frames'][k]['participantFrames'][str(j['participantId'])]['totalGold'])
        except:
            df_creater[x].append(0)
test_df = pd.DataFrame(df_creater, columns=columns)

time_gold = rawdata_df.iloc[0]['timeline']['info']['frames'][5]['participantFrames']['1']['totalGold']  #시간당 골드획득
sum_df = pd.DataFrame(df_creater, columns=columns)
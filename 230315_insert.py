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
import imp
imp.reload(mu)

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

rawdata_df = get_rawdata('SILVER')


# match - ['match_id' 인포,'gameDuration' 인포,'gameVersion' 인포,'summonerName' 인포>파티,'summonerLevel'인포->파티,'participantId'인포->파티
# , 'championName' 인포->파티, 'champExperience' 인포->파티,'teamPosition' 인포->파티, 'teamId' 인포->파티, 'win' 인포->파티,
# 'kills' 인포->파티, 'deaths' 인포->파티 ,'assists' 인포->파티, 'totalDamageDealtToChampions' 인포->파티, 'totalDamageTaken 인포->파티']
# timeline - ['participantId','g5','g6' ~ 'g25'] -> 25분까지 안가는 게임 try except: 0 insert
# columns = ['gameId', 'gameDuration', 'gameVersion', 'summonerName', 'summonerLevel', 'participantId',
#            'championName', 'champExperience',
#            'teamPosition', 'teamId', 'win', 'kills', 'deaths', 'assists', 'totalDamageDealtToChampions',
#            'totalDamageTaken', 'g_5', 'g_6', 'g_7', 'g_8', 'g_9', 'g_10', 'g_11', 'g_12', 'g_13', 'g_14', 'g_15',
#            'g_16', 'g_17',
#            'g_18', 'g_19', 'g_20', 'g_21', 'g_22', 'g_23', 'g_24', 'g_25']
# rawdata_df.iloc[0]['timeline']['info']['frames'][0]['participantFrames']['1']['totalGold']  # 시간당 골드획득

# 원시데이터인 df를 넣어서 ouput match,timeline 데이터가 있는 df를 만들기
# return -> df
# 함수 결과값(df)를 넣을 수 있는 테이블 생성, insert문까지
# pk - (game_id, participantId)
def get_match_timeline_df(df_p):
    df_creater = []
    columns = ['match_id', 'gameDuration', 'gameVersion', 'summonerName', 'summonerLevel', 'participantId',
               'championName', 'champExperience',
               'teamPosition', 'teamId', 'win', 'kills', 'deaths', 'assists', 'totalDamageDealtToChampions',
               'totalDamageTaken', 'g_5', 'g_6', 'g_7', 'g_8', 'g_9', 'g_10', 'g_11', 'g_12', 'g_13', 'g_14', 'g_15',
               'g_16', 'g_17',
               'g_18', 'g_19', 'g_20', 'g_21', 'g_22', 'g_23', 'g_24', 'g_25']
    for m_idx, m in tqdm(enumerate(df_p['matches'])):
        if m['info']['gameDuration'] < 900:
            continue
        for p in m['info']['participants']:
            df_creater.append([
                m['metadata']['matchId'], m['info']['gameDuration'], m['info']['gameVersion'],
                p['summonerName'], p['summonerLevel'], p['participantId'], p['championName'],
                p['champExperience'], p['teamPosition'], p['teamId'], p['win'],
                p['kills'], p['deaths'], p['assists'], p['totalDamageDealtToChampions'],
                p['totalDamageTaken'],
            ])
            for t in range(5, 26):
                try:
                    p_id = str(p['participantId'])
                    g_each = rawdata_df.iloc[m_idx]['timeline']['info']['frames'][t]['participantFrames'][p_id]['totalGold']
                    df_creater[-1].append(g_each)
                except:
                    df_creater[-1].append(0)
    sum_df = pd.DataFrame(df_creater, columns=columns)
    return sum_df

result_df = get_match_timeline_df(rawdata_df)

sql_conn = mu.connect_mysql('lol_icia')
sql_create = '''
CREATE TABLE LOL_MATCHES_TIER ( match_id varchar(20), gameDuration int, gameVersion varchar(20), summonerName varchar(20),
summonerLevel int, participantId int, championName varchar(20), champExperience int, teamPosition varchar(10), teamId int,
win varchar(10), kills int, deaths int, assists int, totalDamageDealtToChampions int, totalDamageTaken int, 
g_5 int, g_6 int, g_7 int, g_8 int, g_9 int, g_10 int, g_11 int, g_12 int, g_13 int, g_14 int, g_15 int, g_16 int,
g_17 int, g_18 int, g_19 int, g_20 int, g_21 int, g_22 int, g_23 int, g_24 int, g_25 int,
CONSTRAINT LMT_PK_ID_PID PRIMARY KEY (match_id, participantId))
'''
mu.mysql_execute(sql_create, sql_conn)
sql_conn.close()

sql_conn = mu.connect_mysql('lol_icia')
tqdm.pandas()

result_df.progress_apply(lambda x: insert(x, sql_conn), axis=1)
mu.mysql_execute_dict('SELECT * FROM LOL_MATCHES_TIER', sql_conn)
sql_conn.commit()
sql_conn.close()
def insert(t, conn):
    sql_insert = (f'insert into LOL_MATCHES_TIER (match_id, gameDuration, gameVersion, summonerName, summonerLevel, '
                  f'participantId, championName, champExperience, teamPosition, teamId, win, kills, deaths,'
                  f'assists, totalDamageDealtTochampions, totalDamageTaken, g_5, g_6, g_7, g_8, g_9, g_10,'
                  f'g_11, g_12, g_13, g_14, g_15, g_16, g_17, g_18, g_19, g_20, g_21, g_22, g_23, g_24, g_25)'
                  f'VALUES({repr(t.match_id)}, {t.gameDuration}, {repr(t.gameVersion)}, '
                  f'{repr(t.summonerName)}, {t.summonerLevel}, {t.participantId}, {repr(t.championName)}, '
                  f'{t.champExperience}, {repr(t.teamPosition)}, {t.teamId}, {repr(str(t.win))}, {t.kills}, '
                  f'{t.deaths}, {t.assists}, {t.totalDamageDealtToChampions}, {t.totalDamageTaken}, '
                  f'{t.g_5}, {t.g_6}, {t.g_7}, {t.g_8}, {t.g_9}, {t.g_10}, '
                  f'{t.g_11}, {t.g_12}, {t.g_13}, {t.g_14}, {t.g_15}, {t.g_16}, '
                  f'{t.g_17}, {t.g_18}, {t.g_19}, {t.g_20}, {t.g_21}, {t.g_22}, '
                  f'{t.g_23}, {t.g_24}, {t.g_25}) '
                  )
    mu.mysql_execute(sql_insert, conn)




import requests
import pandas as pd

riot_api_key = 'RGAPI-f44ca538-b547-401d-a6bc-5ee964133c0f'
url = f'https://kr.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5?api_key={riot_api_key}'
res = requests.get(url).json()
res.keys()
res['entries'][0]['summonerId']

id_list = [i['summonerId'] for i in res['entries']]

# for i in res['entries']:
#     id_list.append(i['summonerId'])

# id_list = list(map(lambda x: x['summonerId'], res['entries']))
id_list

summoner_url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/{id_list[0]}?api_key={riot_api_key}'
summoner_res = requests.get(summoner_url).json()

summoner_res

matches_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{summoner_res["puuid"]}/ids?type=ranked&start=0&count=20&api_key={riot_api_key}'
matches_res = requests.get(matches_url).json()

matches_res

match_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{matches_res[0]}?api_key={riot_api_key}'
match_res = requests.get(match_url).json()

match_res['metadata']
len(match_res['info']['participants'])
match_res['info']['participants'][0]

timeline_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{matches_res[0]}/timeline?api_key={riot_api_key}'
timeline_res = requests.get(timeline_url).json()

timeline_res.keys()
timeline_res['metadata']
timeline_res['info']['frames'][0].keys()
timeline_res['info']['frames'][5]['events']
timeline_res['info']['frames'][5]['participantFrames']

# puuid 를 얻어야 함 아이디를 통해서 : summoner-v4
# match id 얻어야함 puuid를 통해서 : match-v5
# match_id 를 통해서 match 정보나 timeline을 api를 통해 조회 가능

# 3개의 함수를 통해 (puuid를 구하는 함수, matchid를 구하는 함수, match, timeline을 가져오는 함수)
# 자신의 닉네임(롤 아이디)를 작성하면 puuid -> matchesid -> 첫번째 matchid를 사용해서 match,timeline을 가져오는 함수


def match_timeline(summoner_name):

    def get_puuid(summoner_name_p):
        summoner_get_url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner_name_p}?api_key={riot_api_key}'
        summoner_get_res = requests.get(summoner_get_url).json()
        summoner_puuid = summoner_get_res['puuid']
        result_p = get_matches_id(summoner_puuid)
        return result_p

    def get_matches_id(puuid):
        get_matches_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?type=ranked&start=0&count=20&api_key={riot_api_key}'
        get_matches_res = requests.get(get_matches_url).json()
        result_p = get_match_info(get_matches_res[0])
        return result_p

    def get_match_info(match_id):
        get_match_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={riot_api_key}'
        get_match_res = requests.get(get_match_url).json()
        get_timeline_url = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline?api_key={riot_api_key}'
        get_timeline_res = requests.get(get_timeline_url).json()
        result_p = {'match': get_match_res, 'timeline': get_timeline_res}
        return result_p

    result = get_puuid(summoner_name)
    return result


def_test = match_timeline('우리 같은 사람들')
def_test['match']
def_test['timeline']

g_url = f'https://kr.api.riotgames.com/lol/league/v4/grandmasterleagues/by-queue/RANKED_SOLO_5x5?api_key={riot_api_key}'
g_res = requests.get(g_url).json()
len(g_res['entries'])

import time

# 컴프리헨션
start = time.time()
summoner_name_lst = [s['summonerName'] for s in res['entries']]
print(time.time()-start)

# 람다
start = time.time()
summoner_name_lst = list(map(lambda x: x['summonerName'], res['entries']))
print(time.time()-start)

# for문
start = time.time()
summoner_name_lst = []
for i in g_res['entries']:
    summoner_name_lst.append(i['summonerName'])
print(time.time()-start)

from tqdm import tqdm
import pandas as pd

summoner_name_lst

df_creates = []
for s in tqdm(summoner_name_lst[:5]):
    try:
        m_t = match_timeline(s)
        time.sleep(1)
        df_creates.append([m_t['match']['info']['gameId'], m_t['match'], m_t['timeline']])
    except:
        continue

df = pd.DataFrame(df_creates, columns=['gameid', 'matches', 'timeline'])


df.iloc[0].matches['info']['participants'][0].keys()

# for문을 활용해 df안의 모든 matches 컬럼을 돌아서
# [match_id, championName, kills, deaths, assists] 생성
# kda라는 리스트 안에 리스트로 데이터 넣기
# kda_df 라는 DataFram 만들기

df['matches'][0]['metadata']

kda = []
for idx, i in enumerate(df['matches']):
    for j in df.iloc[idx].matches['info']['participants']:
        kda.append([
                    i['metadata']['matchId'],
                    j['championName'],
                    j['kills'],
                    j['deaths'],
                    j['assists'],
                    ])
kda_df = pd.DataFrame(kda, columns=['match_id', 'championName', 'kills', 'deaths', 'assists'])
kda_df['kda'] = round((kda_df.kills+kda_df.assists)/kda_df.deaths, 1)
kda_df

import numpy as np
kda_df['kda'][kda_df.kda == np.inf] = 'Perfect'

import my_utils as mu

# Oracle
oracle_query = """
CREATE TABLE LOL_KDA (GAMEID VARCHAR(20), CHAMPIONNAME VARCHAR(30), KILLS NUMBER(10),
DEATHS NUMBER(20), ASSISTS NUMBER(20), KDA VARCHAR(20))
"""
mu.oracle_open()
mu.oracle_execute(oracle_query)
mu.oracle_close()

# MySQL
mysql_query = """
CREATE TABLE LOL_KDA (GAMEID VARCHAR(20), CHAMPIONNAME VARCHAR(30), KILLS INT(10),
DEATHS INT(20), ASSISTS INT(20), KDA VARCHAR(20))
"""
sql_conn = mu.connect_mysql('lol_icia')
mu.mysql_execute(mysql_query, sql_conn)
sql_conn.close()


def insert(x, sql_conn_p):
    query = (
        f'insert into lol_kda(gameid,championName,kills,deaths,assists,kda)'
        f'values({repr(x.match_id)}, {repr(x.championName)}, {x.kills}, {x.deaths}, {x.assists},' 
        f'{repr(x.kda)})'
        )
    mu.oracle_execute(query)
    mu.mysql_execute(query, sql_conn_p)
    return

tqdm.pandas()
mu.oracle_open()
sql_conn = mu.connect_mysql('lol_icia')

kda_df.progress_apply(lambda x: insert(x, sql_conn),  axis=1)

a1 = mu.oracle_execute('select * from lol_kda')
a1

a2 = pd.DataFrame(mu.mysql_execute_dict('select * from lol_kda', sql_conn))
a2

mu.oracle_close()
sql_conn.commit()
sql_conn.close()

import my_utils as mu

test_mu = mu.match_timeline('hide on bush', 5)
test_mu.keys()

test_mu['match']['metadata']['matchId']
test_mu['match']
test_mu['timeline']

# riot api master 구간 데이터를 가져오기
# 첫번째 df는 match id, matches, timeline
# matches 컬럼을 이용해서 match_df를 만들기
# gameId, gameDuration, gameVersion, summonerName, sommonerLevel, participantId, championName, champExperience, teamPosition, teamId, win, kills, deaths, assists, totalDamageDealtToChampions, totalDamageTaken

m_url = f'https://kr.api.riotgames.com/lol/league/v4/masterleagues/by-queue/RANKED_SOLO_5x5?api_key={riot_api_key}'
m_res = requests.get(m_url).json()

m_name_list = []
for i in m_res['entries'][:50]:
    m_name_list.append(i['summonerName'])

m_puuid_list = []
for i in m_name_list:
    try:
        m_puuid_list.append(mu.get_puuid(i))
    except:
        continue

m_matchid_list = []
for i in m_puuid_list:
    m_matchid_list.append(mu.get_match_id(i, 2))

m_matchid_list

m_list = []
for idx, i in tqdm(enumerate(m_matchid_list)):
    for j in i:
        matches, timelines = mu.get_matches_timelines(j)
        time.sleep(1)
        m_list.append([j, matches, timelines])


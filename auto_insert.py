import requests
import pandas as pd
import my_utils as mu
from tqdm import tqdm
import random


def get_rawdata(tier_p):
    match tier_p:
        case 'C':
            print(tier_p)
            url_p = f'https://kr.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5?api_key={mu.riot_api_key}'
            res_p = requests.get(url_p).json()
            lst = random.sample(res_p['entries'], 5)
            name_lst = [i['summonerName'] for i in lst]

        case 'GM':
            print(tier_p)
            url_p = f'https://kr.api.riotgames.com/lol/league/v4/grandmasterleagues/by-queue/RANKED_SOLO_5x5?api_key={mu.riot_api_key}'
            res_p = requests.get(url_p).json()
            lst = random.sample(res_p['entries'], 5)
            name_lst = [i['summonerName'] for i in lst]

        case 'MASTER':
            print(tier_p)
            url_p = f'https://kr.api.riotgames.com/lol/league/v4/masterleagues/by-queue/RANKED_SOLO_5x5?api_key={mu.riot_api_key}'
            res_p = requests.get(url_p).json()
            lst = random.sample(res_p['entries'], 5)
            name_lst = [i['summonerName'] for i in lst]

        case _:
            print(tier_p)
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


def get_match_timeline_df(df_p):
    df_creater = []
    columns = [
        'match_id', 'gameDuration', 'gameVersion', 'summonerName', 'summonerLevel', 'participantId', 'championName',
        'champExperience', 'teamPosition', 'teamId', 'win', 'kills', 'deaths', 'assists', 'totalDamageDealtToChampions',
        'totalDamageTaken', 'g_5', 'g_6', 'g_7', 'g_8', 'g_9', 'g_10', 'g_11', 'g_12', 'g_13', 'g_14', 'g_15', 'g_16',
        'g_17', 'g_18', 'g_19', 'g_20', 'g_21', 'g_22', 'g_23', 'g_24', 'g_25'
        ]
    for m_idx, m in tqdm(enumerate(df_p['matches'])):
        if m['info']['gameDuration'] < 900:
            continue
        for p in m['info']['participants']:
            df_creater.append([
                m['metadata']['matchId'],
                m['info']['gameDuration'],
                m['info']['gameVersion'],
                p['summonerName'],
                p['summonerLevel'],
                p['participantId'],
                p['championName'],
                p['champExperience'],
                p['teamPosition'],
                p['teamId'],
                p['win'],
                p['kills'],
                p['deaths'],
                p['assists'],
                p['totalDamageDealtToChampions'],
                p['totalDamageTaken']
            ])
            for t in range(5, 26):
                try:
                    p_id = str(p['participantId'])
                    g_each = df_p.iloc[m_idx]['timeline']['info']['frames'][t]['participantFrames'][p_id]['totalGold']
                    df_creater[-1].append(g_each)
                except:
                    df_creater[-1].append(0)
    sum_df = pd.DataFrame(df_creater, columns=columns)
    return sum_df


def insert(t, conn):
    oracle_insert = (
        f'MERGE INTO LOL_MATCHES_TIER USING DUAL ON(match_id={repr(t.match_id)} and participantId = {t.participantId}) '
        f'WHEN NOT MATCHED THEN '
        f'insert (match_id, gameDuration, gameVersion, summonerName, summonerLevel, '
        f'participantId, championName, champExperience, teamPosition, teamId, win, kills, deaths,'
        f'assists, totalDamageDealtTochampions, totalDamageTaken, g_5, g_6, g_7, g_8, g_9, g_10,'
        f'g_11, g_12, g_13, g_14, g_15, g_16, g_17, g_18, g_19, g_20, g_21, g_22, g_23, g_24, g_25) '
        f'VALUES({repr(t.match_id)}, {t.gameDuration}, {repr(str(t.gameVersion))}, '
        f'{repr(t.summonerName)}, {t.summonerLevel}, {t.participantId}, {repr(t.championName)}, '
        f'{t.champExperience}, {repr(t.teamPosition)}, {t.teamId}, {repr(str(t.win))}, {t.kills}, '
        f'{t.deaths}, {t.assists}, {t.totalDamageDealtToChampions}, {t.totalDamageTaken}, '
        f'{t.g_5}, {t.g_6}, {t.g_7}, {t.g_8}, {t.g_9}, {t.g_10}, '
        f'{t.g_11}, {t.g_12}, {t.g_13}, {t.g_14}, {t.g_15}, {t.g_16}, '
        f'{t.g_17}, {t.g_18}, {t.g_19}, {t.g_20}, {t.g_21}, {t.g_22}, '
        f'{t.g_23}, {t.g_24}, {t.g_25}) '
        )

    sql_insert = (
        f'insert into LOL_MATCHES_TIER (match_id, gameDuration, gameVersion, summonerName, summonerLevel, '
        f'participantId, championName, champExperience, teamPosition, teamId, win, kills, deaths,'
        f'assists, totalDamageDealtTochampions, totalDamageTaken, g_5, g_6, g_7, g_8, g_9, g_10,'
        f'g_11, g_12, g_13, g_14, g_15, g_16, g_17, g_18, g_19, g_20, g_21, g_22, g_23, g_24, g_25) '
        f'VALUES({repr(t.match_id)}, {t.gameDuration}, {repr(str(t.gameVersion))}, '
        f'{repr(t.summonerName)}, {t.summonerLevel}, {t.participantId}, {repr(t.championName)}, '
        f'{t.champExperience}, {repr(t.teamPosition)}, {t.teamId}, {repr(str(t.win))}, {t.kills}, '
        f'{t.deaths}, {t.assists}, {t.totalDamageDealtToChampions}, {t.totalDamageTaken}, '
        f'{t.g_5}, {t.g_6}, {t.g_7}, {t.g_8}, {t.g_9}, {t.g_10}, '
        f'{t.g_11}, {t.g_12}, {t.g_13}, {t.g_14}, {t.g_15}, {t.g_16}, {t.g_17}, {t.g_18}, {t.g_19}, {t.g_20}, {t.g_21},'
        f'{t.g_22}, {t.g_23}, {t.g_24}, {t.g_25}) '
        f'ON DUPLICATE KEY UPDATE '
        f'match_id = {repr(t.match_id)}, gameDuration = {t.gameDuration}, gameVersion = {repr(str(t.gameVersion))}, '
        f'summonerName = {repr(t.summonerName)}, summonerLevel = {t.summonerLevel}, participantId = {t.participantId}, '
        f'championName = {repr(t.championName)}, champExperience = {t.champExperience}, teamPosition = {repr(t.teamPosition)}, '
        f'teamId = {repr(t.teamId)}, win = {repr(str(t.win))}, kills = {t.kills}, deaths = {t.deaths}, assists = {t.assists}, '
        f'totalDamageDealtToChampions = {t.totalDamageDealtToChampions}, totalDamageTaken = {t.totalDamageTaken}, '
        f'g_5 = {t.g_5}, g_6 = {t.g_6}, g_7 = {t.g_7}, g_8 = {t.g_8}, g_9 = {t.g_9}, g_10 = {t.g_10}, g_11 = {t.g_11}, '
        f'g_12 = {t.g_12}, g_13 = {t.g_13}, g_14 = {t.g_14}, g_15 = {t.g_15}, g_16 = {t.g_16}, g_17 = {t.g_17}, g_18 = {t.g_18}, '
        f'g_19 = {t.g_19}, g_20 = {t.g_20}, g_21 = {t.g_21}, g_22 = {t.g_22}, g_23 = {t.g_23}, g_24 = {t.g_24}, g_25 = {t.g_25} '
    )
    try:
        mu.mysql_execute(sql_insert, conn)
        mu.oracle_execute(oracle_insert)
    except Exception as e:
        print(f'insert {e}예외 발생')
        return


def auto_insert(num=int):
    """
    :param num: 반복 횟 수
    """
    tqdm.pandas()
    for count in tqdm(range(num)):
        tier = ['IRON', 'BRONZE', 'SILVER', 'GOLD', 'PLATINUM', 'DIAMOND', 'MASTER', 'GM', 'C']
        idx = random.randrange(len(tier))
        rawdata_df = get_rawdata(tier[idx])
        result_df = get_match_timeline_df(rawdata_df)
        conn = mu.connect_mysql('lol_icia')
        mu.oracle_open()
        result_df.progress_apply(lambda x: insert(x, conn), axis=1)
        conn.commit()
        conn.close()
        mu.oracle_close()
        print(f'반복 {count+1}회 완료')
    print('반복 완료')
    # time.sleep(60)
# ------------------------------------------------------------------------#
# Package
import pandas as pd
import my_utils as mu
import auto_insert as ai
import time
import random
import numpy as np
# ------------------------------------------------------------------------#

raw_data = ai.get_rawdata('DIAMOND')
raw_data

df = ai.get_match_timeline_df(raw_data)
df

raw_data.iloc[0].matches['info']['participants']

tmp = raw_data


# 어시스트 처리
def assi_calc(x):
    try:
        return ','.join(list(map(lambda y: str(y), x['assistingParticipantIds'])))
    except:
        return ''


def lane_processing(tower_lane, tower_team, lane, team):
    if lane == 'TOP':
        lane = 'TOP_LANE'
    elif lane == 'MIDDLE':
        lane = 'MID_LANE'
    elif lane == 'BOTTOM':
        lane = 'BOT_LANE'
    if (tower_lane == lane) and (tower_team == team):
        return 1
    else:
        return 0


df_create = []
for i in range(len(tmp)):
    matchId = tmp.iloc[i].match_id
    match = tmp.iloc[i].matches['info']
    timeline = tmp.iloc[i].timeline['info']
    for j in range(10):
        lst_tmp = []
        lst_tmp.append(matchId)
        lst_tmp.append(match['gameDuration'])
        lst_tmp.append(match['gameVersion'])
        lst_tmp.append(match['participants'][j]['participantId'])
        lst_tmp.append(match['participants'][j]['championName'])
        lst_tmp.append(match['participants'][j]['teamPosition'])
        lst_tmp.append(match['participants'][j]['teamId'])
        lst_tmp.append(match['participants'][j]['win'])
        lst_tmp.append(match['participants'][j]['kills'])
        lst_tmp.append(match['participants'][j]['deaths'])
        lst_tmp.append(match['participants'][j]['assists'])
        lst_tmp.append(match['participants'][j]['totalDamageDealtToChampions'])
        lst_tmp.append(match['participants'][j]['totalDamageTaken'])

        tower_log = list(
            map(lambda x: list(filter(lambda z: z['type'] == 'BUILDING_KILL', x['events'])), timeline['frames']))
        tower_log = [element for array in tower_log for element in array]
        try:
            ft_tower_lane = tower_log[0]['laneType']
            ft_tower_team = tower_log[0]['teamId']
        except:
            ft_tower_lane = 'n'
            ft_tower_team = 'n'
        ft = lane_processing(ft_tower_lane, ft_tower_team, lst_tmp[5], lst_tmp[6])
        lst_tmp.append(ft)

        tower_tmp = list(map(lambda x: (x['laneType'], x['teamId'], x['timestamp']), tower_log))

        try:
            laneTower = list(filter(lambda x: (x[0][0] == lst_tmp[5][0]) & (x[1] == lst_tmp[6]), tower_tmp))[0]
            lane_flag = 1
            laneTowerTime = laneTower[-1]
        except:
            lane_flag = 0
            laneTowerTime = 0
        lst_tmp.append(lane_flag)
        lst_tmp.append(laneTowerTime)

        blue_ban = list(map(lambda x: str(x['championId']), match['teams'][0]['bans']))
        red_ban = list(map(lambda x: str(x['championId']), match['teams'][1]['bans']))
        ban_list = list(set(blue_ban + red_ban))
        lst_tmp.append('|'.join(ban_list))

        kill_log = list(
            map(lambda x: list(filter(lambda z: z['type'] == 'CHAMPION_KILL', x['events'])), timeline['frames']))
        kill_log = [element for array in kill_log for element in array]
        k = list(map(lambda x: str(x['killerId']), kill_log))
        v = list(map(lambda x: str(x['victimId']), kill_log))
        a = list(map(lambda x: assi_calc(x), kill_log))

        lst_tmp.append('|'.join(k))
        lst_tmp.append('|'.join(v))
        lst_tmp.append('|'.join(a))

        try:
            g15 = timeline['frames'][14]['participantFrames'][str(j + 1)]['totalGold']
        except:
            g15 = 0

        lst_tmp.append(g15)
        df_create.append(lst_tmp)

col_lst = ['matchId', 'gameDuration', 'gameVersion', 'participantId', 'championName', 'teamPosition',
           'teamId', 'win', 'kills', 'deaths', 'assists', 'damageDealt', 'damageTaken', 'firstDT',
           'laneTower', 'LaneTowerTime', 'bans', 'killerId', 'victimId', 'assistId', 'G15']
df = pd.DataFrame(df_create, columns=col_lst)

# [gameVersion, matchId, teamPosition, championName,G15] - > rename(CHAMPIONNAME -> enemy_champ,
# G15 -> enemy_g15) -> blue red   red  blue
# merge를 통해서 blue_team과 위의 컬럼들을 가진 df을 합쳐줌
# append 사용해서 red의 데이터를 blue팀 밑에 넣기.
# tmp라는 데이터프레임에 담기

blue_df = df[df['teamId'] == 100]
blue_df = blue_df[['gameVersion', 'matchId', 'teamPosition', 'participantId', 'championName', 'G15', 'teamId']]
red_df = df[df['teamId'] == 200]
red_df = red_df[['gameVersion', 'matchId', 'teamPosition', 'participantId', 'championName', 'G15', 'teamId']]


tmp_df = pd.merge(blue_df, red_df, on=['matchId', 'teamPosition'])
tmp_df.rename(columns={'championName_y': 'enemy_champ', 'G15_y': 'enemy_G15'}, inplace=True)
tmp_df2 = pd.merge(red_df, blue_df, on=['matchId', 'teamPosition'])
tmp_df2.rename(columns={'championName_y': 'enemy_champ', 'G15_y': 'enemy_G15'}, inplace=True)
tmp = tmp_df.append(tmp_df2)

# lane_win 이라는 컬럼을 만들어서 g_15 > enemy_g15일 경우에는 1 반대일 경우에는 0을 입력
# first_blood 라는 컬럼을 만들어서 killerId 컬럼에서 firstblood를 한 participants 번호일 경우 1 나머지 0
# kill_point라는 컬럼을 만들어서 (kills + assists /같은팀의 총 킬수)를 입력 (killerId를 이용하는방법 or kills를 이용하는 방법 )

def lane_win(tmp_p):

    if tmp_p.G15_X > tmp_p.enemy_G15:
        return 1
    else:
        return 0


tmp['lane_win'] = tmp.apply(lambda x: 1 if(x['G15_x'] > x['enemy_G15']) else 0, axis=1)

def first_blood(x):
    for idx, i in enumerate(df['matchId']):
        if x.matchId == i:
            # print(df.iloc[idx]['killerId'].split('|')[0], x.participantId_x)
            if df.iloc[idx]['killerId'][0] == str(x.participantId_x):
                return 1
            else:
                return 0


tmp['first_blood'] = tmp.apply(lambda x: first_blood(x), axis=1)

def kill_point(x):
    sum = 0
    avg = 0
    for idx, i in enumerate(df['matchId']):
        if x.matchId == i:
            if x.teamId_x == df.iloc[idx]['teamId'] and x.participantId_x == df.iloc[idx]['participantId']:
                sum = df.iloc[idx]['kills'] + df.iloc[idx]['assists']
            if x.teamId_x == df.iloc[idx]['teamId']:
                avg = avg + df.iloc[idx]['kills']
                print(avg)
    return round(sum / avg * 100, 2)


tmp['kill_point'] = tmp.apply(lambda x: kill_point(x), axis=1)

tmp['kill_point']
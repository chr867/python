import my_utils as mu
import pandas as pd
import auto_insert as ai
import imp
raw_data = ai.get_rawdata('DIAMOND')

tmp_lst = list(map(lambda x: x['events'], raw_data.iloc[0]['timeline']['info']['frames']))

event_lst = [element for array in tmp_lst for element in array]
tower_log = [i for i in event_lst if i['type'] == 'BUILDING_KILL']


df_creater_test = []
for idx, i in enumerate(raw_data['matches']):
    gameId = i['info']['gameId']
    gameDuration = i['info']['gameDuration']
    gameVersion = i['info']['gameVersion']
    for j in range(10):
        # participantId
        participantId = i['info']['participants'][j]['participantId']
        teamId = i['info']['participants'][j]['teamId']
        teamPosition = i['info']['participants'][j]['teamPosition']
        
        # bans
        blue_test = [str(i['championId']) for i in i['info']['teams'][0]['bans']]
        red_test = [str(i['championId']) for i in i['info']['teams'][1]['bans']]
        ban_test = list(set(blue_test + red_test))
        bans = '|'.join(ban_test)

        # CHMAPION_KILL
        events_test = [i['events'] for i in raw_data.iloc[idx]['timeline']['info']['frames']]
        tmp_lst2 = [element for array in events_test for element in array]

        # firstDT
        tower_log = [i for i in tmp_lst2 if i['type'] == 'BUILDING_KILL']
        try:
            ft_tower_lane = tower_log[0]['laneType']
            ft_tower_team = tower_log[0]['teamId']
        except:
            ft_tower_lane = 'n'
            ft_tower_team = 'n'
        match teamPosition:
            case 'TOP':
                lane = 'TOP_LANE'
            case 'MIDDLE':
                lane = 'MID_LANE'
            case 'BOTTOM':
                lane = 'BOT_LANE'
            case _:
                lane = ''
        if ft_tower_lane == lane and ft_tower_team == teamId:
            firstDT = 1
        else:
            firstDT = 0

        # laneTower
        laneTower = 0
        laneTowerTime = 0
        for k in tower_log:
            if k['buildingType'] == 'TOWER_BUILDING':
                if k['laneType'] == lane and k['teamId'] == teamId:
                    laneTower = 1
                    laneTowerTime = k['timestamp']

        # kill
        kill_log = [i for i in tmp_lst2 if i['type'] == 'CHAMPION_KILL']
        k = [i['killerId'] for i in kill_log]
        d = [i['victimId'] for i in kill_log]
        k_lst = '|'.join(map(str, k))
        d_lst = '|'.join(map(str, d))

        def assist(event):
            try:
                return event['assistingParticipantIds']
            except:
                return ' '

        a = [assist(i) for i in kill_log]
        a_lst = '|'.join(map(str, a))
        df_creater_test.append([gameId, gameDuration, gameVersion, participantId, teamPosition, teamId, firstDT,
                                laneTower, laneTowerTime, bans, k_lst, d_lst, a_lst])

col_lst = ['gameId', 'gameDuration', 'gameVersion', 'participantId', 'teamPosition', 'teamId', 'firstDT', 'laneTower',
           'laneTowerTime', 'bans', 'killerId', 'victimId', 'assistId']
result_df = pd.DataFrame(df_creater_test, columns=col_lst)


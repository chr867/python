from tqdm import tqdm
import my_utils as mu
import auto_insert as ai
import pandas as pd
tqdm.pandas()
raw_data = ai.get_rawdata('DIAMOND')

# lambda를 활용해서  blue_ban, red_ban 리스트 생성
# 이후 두개의 리스트를 합침(중복제거)
# 리스트 안의 요소를 |로 묶어서 str형태로 만들기

raw_data.iloc[0].matches['info']['teams'][0]['bans']

blue_team = list(map(lambda x: str(x['championId']), raw_data.iloc[0].matches['info']['teams'][0]['bans']))
red_team = list(map(lambda x: str(x['championId']), raw_data.iloc[0].matches['info']['teams'][1]['bans']))

ban = list(set(blue_team + red_team))
ban_lst = '|'.join(ban)

# 킬 | 죽은사람 | 어시스트 ,
# 6  |    3   | 8,9,10

raw_data.iloc[0]['timeline']['info']['frames'][1].keys()

# 가장 첫번째 있는 게임의 챔피언 킬 정보
# events 라는 키의 벨류들을 lambda filter를 사용해서 하나의 리스트로 묶기
# 이중 리스트를 하나로 풀기
# lambda를 통해서 events의 type이 CHAMPION_KILL인 것들만 kill_log라는 리스트에 담기
# kill_log 에서 killerId의 값을 list로 묶기 -> k
# kill_log 에서 victimId 값을 list로 묶기 -> d
# kill_log 에서 assistingParticipantIds 값을 list로 묶기 -> a
# 세개의 리스트를 각각 |로 묶기
# col_lst = ['gamdId', 'gameDuration', 'gameVersion', 'bans', 'killerId', 'victimId', 'assistId'] 컬럼인 데이터 프레임

events = list(map(lambda x: x['events'], raw_data.iloc[0]['timeline']['info']['frames']))
events


tmp_lst = []
for i in events:
    tmp_lst += i


def assist(event):
    try:
        return event['assistingParticipantIds']
    except:
        return ' '

kill_log = [i for i in tmp_lst if i['type'] == 'CHAMPION_KILL']

k = [i['killerId'] for i in kill_log]
d = [i['victimId'] for i in kill_log]
a = [assist(i) for i in kill_log]

gameId = raw_data.iloc[0]['matches']['info']['gameId']
gameDuration = raw_data.iloc[0]['matches']['info']['gameDuration']
gameVersion = raw_data.iloc[0]['matches']['info']['gameVersion']
ban_lst
k_lst = '|'.join(map(str, k))
d_lst = '|'.join(map(str, d))
a_lst = '|'.join(map(str, a))
col_lst = ['gamdId', 'gameDuration', 'gameVersion', 'bans', 'killerId', 'victimId', 'assistId']

df_creater = [[gameId, gameDuration, gameVersion, ban_lst, k_lst, d_lst, a_lst]]
result_df = pd.DataFrame(df_creater, columns=col_lst)


def get_event(df):
    df_creater_test = []
    for idx, i in enumerate(df['matches']):
        gameId = i['info']['gameId']
        gameDuration = i['info']['gameDuration']
        gameVersion = i['info']['gameVersion']
        for j in range(10):
            # bans
            blue_test = [str(i['championId']) for i in i['info']['teams'][0]['bans']]
            red_test = [str(i['championId']) for i in i['info']['teams'][1]['bans']]
            ban_test = list(set(blue_test + red_test))
            bans = '|'.join(ban_test)

            # CHMAPION_KILL
            # events_test = list(map(lambda x: x['events'], df.iloc[idx]['timeline']['info']['frames']))
            events_test = [i['events'] for i in df.iloc[idx]['timeline']['info']['frames']]
            tmp_lst2 = [element for array in events_test for element in array]

            kill_log = [i for i in tmp_lst2 if i['type'] == 'CHAMPION_KILL']
            # kill_log = list(filter(lambda x:x['type'] == 'CHAMPION_KILL', tmp_lst2))
            # kill_log = list(map(lambda x: list(filter(lambda z: z['type'] == 'CHAMPION_KILL', x['events'])), df.iloc[idx]['timeline']['info']['frames']))
            # kill_log = [element for array in kill_log for element in array]

            # k = list(map(lambda x: str(x['killerId']), kill_log))
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
            df_creater_test.append([gameId, gameDuration, gameVersion, bans, k_lst, d_lst, a_lst])
    col_lst = ['gamdId', 'gameDuration', 'gameVersion', 'bans', 'killerId', 'victimId', 'assistId']
    result_df = pd.DataFrame(df_creater_test, columns=col_lst)
    return result_df

test_df = get_event(raw_data)


import pandas as pd
import my_utils as mu
import auto_insert as ai
from tqdm import tqdm

# 챔피언 시너지 또는 2:2 구도 분석
mu.oracle_open()
df = mu.oracle_execute('select * from lol_datas')
df = df[(df.GAMEDURATION > 900) & (df.GAMEDURATION < 100000)]

df['win'] = df.apply(lambda x: 1 if x.WIN == 'True' else 0, axis=1)

# blue팀 정글포지션 사람들만 꺼내서 [GAMEID,CHAMPIONNAME,WIN].RENAME(CHAMPIONID -> j_champ) \
#   (테이블 이름 - blue_jungle)
# blue팀 미드포지션 사람들만 꺼내서 [GAMEID,CHAMPIONNAME,WIN].RENAME(CHAMPIONID -> m_champ)
#    (테이블 이름 - blue_middle)
# 둘이 merge
#    blue_team
# red팀도 동일한 포지션 동일하게 merge
#    red_jungle
#    red_top
#    red_team

# 위에서 merge한 것을 각각 또 새롭게 테이블을 만들어서 enemy_j_champ, enemy_m_champ로 rename
# blue_tmp = blue_team.rename(enemy_j_champ,enemy_m_champ)
# red_tmp

# red팀과 blue팀에 enemy_champ들을 merge해서 append
#    tmp = merge()

# games와 win횟수를 구하고 win_rate 컬럼 추가한  result 테이블 만들기
#    result =

blue_df_jg = df[(df.TEAMID == 100) & (df.TEAMPOSITION == 'JUNGLE')][['GAMEID', 'CHAMPIONNAME', 'win']].rename(columns={'CHAMPIONNAME': 'j_champ'})
blue_df_mid = df[(df.TEAMID == 100) & (df.TEAMPOSITION == 'MIDDLE')][['GAMEID', 'CHAMPIONNAME', 'win']].rename(columns={'CHAMPIONNAME': 'm_champ'})
blue_df_mg = pd.merge(blue_df_mid, blue_df_jg, on=['GAMEID', 'win'])

red_df_jg = df[(df.TEAMID == 200) & (df.TEAMPOSITION == 'JUNGLE')][['GAMEID', 'CHAMPIONNAME', 'win']].rename(columns={'CHAMPIONNAME': 'j_champ'})
red_df_mid = df[(df.TEAMID == 200) & (df.TEAMPOSITION == 'MIDDLE')][['GAMEID', 'CHAMPIONNAME', 'win']].rename(columns={'CHAMPIONNAME': 'm_champ'})
red_df_mg = pd.merge(red_df_mid, red_df_jg, on=['GAMEID', 'win'])

blue_tmp = blue_df_mg.rename(columns={'j_champ': 'enemy_j_champ', 'm_champ': 'enemy_m_champ'})
red_tmp = red_df_mg.rename(columns={'j_champ': 'enemy_j_champ', 'm_champ': 'enemy_m_champ'})

blue_tmp2 = pd.merge(blue_df_mg, red_tmp, on='GAMEID')
red_tmp2 = pd.merge(red_df_mg, blue_tmp, on='GAMEID')

tmp = blue_tmp2.append(red_tmp2)
tmp = tmp[['m_champ', 'j_champ', 'enemy_j_champ', 'enemy_m_champ', 'win_x']]
count_df = tmp.groupby(['m_champ', 'j_champ', 'enemy_m_champ', 'enemy_j_champ']).count().rename(
    columns={'win_x': 'cnt'})

count_df[count_df['cnt'] > 10]

win_cnt_df = tmp.groupby(['m_champ', 'j_champ', 'enemy_m_champ', 'enemy_j_champ']).sum().rename(
    columns={'win_x': 'win_cnt'})

result = count_df.join(win_cnt_df)

result['win_rate'] = result.win_cnt/result.cnt
result[result['cnt'] > 10]

blue_df_ad = df[(df.TEAMID == 100) & (df.TEAMPOSITION == 'BOTTOM')].rename(columns={'CHAMPIONNAME': 'adc_champ'})
blue_df_sup = df[(df.TEAMID == 100) & (df.TEAMPOSITION == 'UTILITY')].rename(columns={'CHAMPIONNAME': 'sup_champ'})
blue_df_mg2 = pd.merge(blue_df_ad, blue_df_sup, on=['GAMEID'])

red_df_ad = df[(df.TEAMID == 200) & (df.TEAMPOSITION == 'BOTTOM')].rename(columns={'CHAMPIONNAME': 'adc_champ'})
red_df_sup = df[(df.TEAMID == 200) & (df.TEAMPOSITION == 'UTILITY')].rename(columns={'CHAMPIONNAME': 'sup_champ'})
red_df_mg2 = pd.merge(red_df_ad, red_df_sup, on=['GAMEID'])

blue_tmp2 = blue_df_mg2.rename(columns={'adc_champ': 'enemy_adc_champ', 'sup_champ': 'enemy_sup_champ'})
red_tmp2 = red_df_mg2.rename(columns={'adc_champ': 'enemy_adc_champ', 'sup_champ': 'enemy_sup_champ'})

blue_tmp2 = pd.merge(blue_df_mg2, red_tmp2, on='GAMEID')
red_tmp2 = pd.merge(red_df_mg2, blue_tmp2, on='GAMEID')
tmp2 = blue_tmp2.append(red_tmp2)
tmp2 = tmp2[['adc_champ',  'sup_champ', 'win_x_x', 'enemy_adc_champ', 'enemy_sup_champ']]

count_df2 = tmp2.groupby(['adc_champ', 'sup_champ', 'enemy_adc_champ', 'enemy_sup_champ']).count().rename(
    columns={'win_x_x': 'cnt'})

win_cnt_df2 = tmp2.groupby(['adc_champ', 'sup_champ', 'enemy_adc_champ', 'enemy_sup_champ']).sum().rename(
    columns={'win_x_x': 'win_cnt'})

result2 = count_df2.join(win_cnt_df2)
result2['win_rate'] = result2.win_cnt/result2.cnt

tmp2[tmp2['adc_champ'] == 'Kaisa']
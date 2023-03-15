import numpy as np
import pandas as pd
import my_utils as mu
import requests
import matplotlib.pyplot as plt
# from matplotlib import font_manager, rc
# conda install matplotlib

url = 'http://openAPI.seoul.go.kr:8088/(인증키)/xml/TimeAverageAirQuality/1/5/20220615/종로구'
j_df = mu.df_creater(url)

j_df.drop(columns='MSRSTE_NM', inplace=True)
# j_df.drop(['MSRSTE_NM'], axis=1, inplace=True)

j_df.set_index('MSRDT', inplace=True)

j_df.plot(figsize=(15, 5), legend=True, marker='o', rot=0)
plt.grid(True)
plt.xlabel('시간')
plt.ylabel('수치')
plt.title('종로구 시간별 오염 수치')

from matplotlib import font_manager, rc
font_path = "C:/Windows/Fonts/gulim.ttc"
font = font_manager.FontProperties(fname=font_path).get_name()
rc('font', family=font)

# 'NO2', 'O3', 'SO2' 만 사용해서 그래프 만들기

j_df2 = j_df[['NO2', 'O3', 'SO2']]
j_df2 = j_df2[::-1]

j_df2.plot(figsize=(15, 5), legend=True, marker='x', rot=0)
plt.grid(True)
plt.xlabel('시간')
plt.ylabel('수치')
plt.title('종로구 시간별 오염 수치')

plt.rc('font', size=20)
plt.rc('axes', labelsize=15)

j_df.reset_index(inplace=True)

xdata = j_df.NO2
ydata = j_df.PM10
plt.figure()
plt.plot(xdata, ydata, color='r', marker='o', linestyle='None')
plt.xlabel('이산화질소 농도')
plt.ylabel('미세먼지')
plt.title('산점도 그래프')
plt.grid(True)

ydata2 = j_df.PM25
plt.figure()
plt.plot(xdata, ydata, color='r', marker='o', linestyle='None')
plt.plot(xdata, ydata2, color='y', marker='o', linestyle='None')
plt.xlabel('이산화질소 농도')
plt.ylabel('(초)미세먼지')
plt.title('산점도 그래프')
plt.grid(True)

# bar 그래프
j_df3 = j_df[::-1]
j_df3[:7]
plt.figure()
plt.bar(j_df3.MSRDT[:7], j_df3.PM10[:7], color=['b', 'g', 'r', 'c', 'm', 'y', 'k'])
plt.xticks(rotation=45)
plt.xlabel('시간')
plt.ylabel('미세먼지')
plt.title('시간별 미세먼지 막대 그래프')

mean_val = j_df3.PM10[:7].mean()
plt.axhline(y=mean_val, color='r', linewidth=1, linestyle='dashed')

# subflot
fig = plt.figure(figsize=(16, 10), dpi=80)
grid = plt.GridSpec(4, 4, hspace=0.5, wspace=0.2)

ax_main = fig.add_subplot(grid[:-1, :-1])
ax_right = fig.add_subplot(grid[:-1, -1], xticklabels=[], yticklabels=[])
ax_bottom = fig.add_subplot(grid[-1, 0:-1], xticklabels=[], yticklabels=[])

ax_main.scatter('NO2', 'PM10', color='r', data=j_df)
ax_right.hist(j_df.PM10, 40, orientation='horizontal', color='lightblue')
ax_bottom.hist(j_df.NO2, 40, orientation='vertical', color='lightpink')

mu.oracle_open()
sql_conn = mu.connect_mysql('lol_icia')

lol_df = mu.oracle_execute('select * from lol_matches')
lol_df = pd.DataFrame(mu.mysql_execute_dict('select * from lol_matches', sql_conn))

tmp_df = lol_df[['teamPosition', 'totalDamageDealtToChampions', 'totalDamageTaken', 'kills']]
tmp_df = tmp_df[~tmp_df.teamPosition.isna()]  # na를 걸러내
jg_df = tmp_df[tmp_df.teamPosition == 'JUNGLE']
jg_df

xdata = jg_df.kills
ydata = jg_df.totalDamageDealtToChampions

plt.figure()
plt.plot(xdata, ydata, color='y', marker='o', linestyle='None')
plt.xlabel('킬')
plt.ylabel('딜량')
plt.title('딜량에 따른 킬 수 관계')
plt.grid(True)

ydata2 = jg_df.totalDamageTaken
plt.figure()
plt.plot(xdata, ydata, color='r', marker='o', linestyle='None')
plt.plot(xdata, ydata2, color='y', marker='x', linestyle='None')
plt.xlabel('킬')
plt.ylabel('딜량')
plt.title('가한 피해량,받은 피해량과 킬 수')
plt.grid(True)


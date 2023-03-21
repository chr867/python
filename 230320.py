import pandas as pd
import my_utils as mu
import matplotlib.pyplot as plt
from matplotlib import font_manager, rc
import seaborn as sns
import plotly

font_path = "C:/Windows/Fonts/gulim.ttc"
font = font_manager.FontProperties(fname=font_path).get_name()
rc('font', family=font)  # 윈도우

mu.oracle_open()
df = mu.oracle_execute('select * from lol_matches_tier')
mu.oracle_close()

df
tmp_df = df[['TEAMPOSITION', 'TOTALDAMAGEDEALTTOCHAMPIONS']]
tmp_df.set_index('TEAMPOSITION', inplace=True)
tmp_df[:10000]

plt.figure(figsize=(8, 6))
plt.hist(tmp_df, bins=len(tmp_df), label='모든 라인', rwidth=0.9)
plt.xlabel('딜량', size=14)
plt.ylabel('빈도수', size=14)
plt.title('딜량 히스토그램')
plt.legend(loc='upper right')
plt.grid(axis='y', alpha=0.3)

# conda install seaborn
# import seaborn as sns
# 에러 뜰 시 numpy 삭제 conda uninstall ~~

url = 'http://openapi.seoul.go.kr:8088/(인증키)/xml/GetParkInfo/1/5/'
tmp_df2 = mu.df_creater(url)
seoul_df = tmp_df2[['PARKING_NAME', 'CAPACITY', 'PAY_NM', 'SATURDAY_PAY_NM',
                    'HOLIDAY_PAY_NM', 'FULLTIME_MONTHLY', 'RATES', 'ADD_RATES']]
seoul_df = seoul_df.drop_duplicates()

seoul_df.columns = ['주차장명', '주차수', '유무료', '주말유무료', '공휴일유무료',
                    '월정기권금액', '기본요금', '추가요금']

plt.figure()
plt.title('주차장 유무료')
sns.countplot(x='유무료', data=seoul_df, order=['유료', '무료'])

plt.figure()
plt.title('기본요금 히스토그램')
sns.histplot(x=seoul_df['기본요금'])

sns.histplot(x=seoul_df['기본요금'], y=seoul_df['추가요금'])

sns.kdeplot(x=seoul_df['기본요금'])
tmp_df3 = df[df['TEAMPOSITION'], df['G_15'], df['G_20']]
sns.kdeplot(x=tmp_df3[tmp_df3['G_20'] != 0['G_20']])
sns.kdeplot(x=tmp_df3[tmp_df3['G_15'] != 0['G_15']])

sns.displot(x=seoul_df['추가요금'], kind='ecdf')
sns.rugplot(x=seoul_df['기본요금'])

sns.barplot(x=seoul_df['주말유무료'], y=seoul_df['기본요금'])

sns.violinplot(x=seoul_df['주말유무료'], y=seoul_df['기본요금'])

sns.stripplot(x=seoul_df['주말유무료'], y=seoul_df['기본요금'])

sns.heatmap(seoul_df.corr(), annot=True, cmap='viridis')

sns.clustermap(seoul_df.corr(), annot=True, cmap='viridis')

sns.FacetGrid(seoul_df, col='주말유무료', row='공휴일유무료').map(sns.displot, '기본요금')

sns.jointplot(x=seoul_df['기본요금'], y=seoul_df['추가요금'], kind='scatter')
sns.jointplot(x=seoul_df['기본요금'], y=seoul_df['추가요금'], kind='hex')

sns.pairplot(seoul_df)

sns.regplot(x='기본요금', y='추가요금', data=seoul_df)

sns.lmplot(x='기본요금', y='추가요금', data=seoul_df, hue='주말유무료')

sns.relplot(x='주차수', y='추가요금', hue='기본요금', row='주말유무료', col='공휴일유무료', data=seoul_df)

# conda install plotly
# import plotly
import plotly.graph_objs as go
help(plotly)
plotly.offline.iplot({
    "data": [go.Scatter(x=[1, 2, 3, 4], y=[4, 3, 2, 1])],
    "layout": go.Layout(title='hello world')
})

# API를 통해서 라이엇 데이터 수집하기(로우데이터 DF만들기) - 티어는 자유
# 불러온 데이터를 전처리하기(중복제거, na값 제거) matches,timeline 합쳐서 최소컬럼 25개
# 위의 과정을 함수화해서 모듈 만들기
# 전처리된 로우데이터를 ORACLE 혹은 MYSQL에 저장하기 -> 최소 5만개
# 팀과 함께 상의해서 흥미롭고 재밌을 것 같은 주제 3가지 선정
# 지표 분석을 통해 나온 df를 시각화 모듈을 사용해서 표현 (3개의 주제 중 2개)
# 전처리부터 시각화까지 캡쳐해서 폴더에 저장
# 개발 보고서, ppt를 제작 후 간단하게 발표
# 모든 py파일, ipynb파일, 사진, 개발 보고서, ppt 하나로 압축
# appfaa6@gmail.com
# 목요일 오전 까지 오후에 발표




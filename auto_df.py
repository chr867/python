import my_utils as mu
import pandas as pd
# 자동

# df의 컬럼명 추출


def df_fullauto(df_name, table_name):
    def df_automatic(dff):
        dff_keys = []
        for idx, i in enumerate(dff.keys()):
            dff_keys.append(i)
        return list(dff_keys)
    df_automatic(df_name)

    # create table
    test_query = "CREATE TABLE "+table_name+"("   # 테이블명,df이름 인자로 지정
    for idx, i in enumerate(df_automatic(df_name)):  # 컬럼 추출 메소드를 이용 create 쿼리문
        if idx == len(df_automatic(df_name))-1:
            test_query += i+' varchar(50))'
        else:
            test_query += i+' varchar(50),'
    test_query
    sql_conn = mu.connect_mysql('lol_icia')
    mu.mysql_execute(test_query, sql_conn)


    # insert
    first_query = 'INSERT INTO '+table_name+'('

    for idx, i in enumerate(df_automatic(df_name)):
        if idx == len(df_automatic(df_name))-1:
            first_query += i+')'
        else:
            first_query += i+', '
    first_query

    for i in range(len(df_name)):  # 컬럼 추출 메소드, iloc를 이용해 각 튜플의 밸류 설정 후 insert
        second_query = ' VALUES ('
        for idx, j in enumerate(df_automatic(df_name)):
            if idx == len(df_automatic(df_name)) - 1:
                second_query += repr(str(df_name[j].iloc[i])) + ")"
                def_test_query = first_query+second_query
                mu.mysql_execute(def_test_query, sql_conn)
            else:
                second_query += repr(str(df_name[j].iloc[i])) + ', '

    sql_conn.commit()
    pd.DataFrame(mu.mysql_execute('select * from '+table_name, sql_conn))
    sql_conn.close()


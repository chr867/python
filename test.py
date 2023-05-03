import my_utils as mu
import imp
imp.reload(mu)
sql_conn = mu.connect_mysql('my_db')

def has_duplicates(lst):
    seen = set()
    for item in lst:
        if tuple(map(tuple, item)) in seen:
            return True
        seen.add(tuple(map(tuple, item)))
    return False

# 입력으로 주어진 이중 리스트
lists = [
    [['GOLD', 'IV'], ['IRON', 'IV'], ['IRON', 'III']],
    [['SILVER', 'IV'], ['IRON', 'II'], ['PLATINUM', 'II']],
    [['SILVER', 'II'], ['IRON', 'I'], ['PLATINUM', 'III']],
    [['SILVER', 'III'], ['GOLD', 'I'], ['DIAMOND', 'IV']],
    [['SILVER', 'I'], ['GOLD', 'II'], ['DIAMOND', 'I']],
    [['BRONZE', 'II'], ['BRONZE', 'III'], ['DIAMOND', 'III']],
    [['GOLD', 'III'], ['BRONZE', 'I'], ['DIAMOND', 'II']],
    [['BRONZE', 'IV'], ['PLATINUM', 'IV'], ['PLATINUM', 'I']]
]

# 중복된 요소가 있는지 여부 확인 후 결과 출력
if has_duplicates(lists):
    print("입력으로 주어진 이중 리스트에 중복된 요소가 있습니다.")
else:
    print("입력으로 주어진 이중 리스트에 중복된 요소가 없습니다.")


for i in lists[0]:
    print(i)

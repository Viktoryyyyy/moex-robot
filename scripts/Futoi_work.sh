source ~/moex_bot/venv/bin/activate
python - << 'PY'
import requests, pandas as pd, re
from datetime import date
from moexalgo import session, Ticker
import pathlib

# 1) Токен
session.TOKEN = (pathlib.Path.home() / '.moex_token').read_text().strip()

# 2) Период
start = '2025-10-01'
end = date.today().isoformat()

# 3) Список срочных инструментов
url = 'https://iss.moex.com/iss/engines/futures/markets/forts/securities.json'
params = {'iss.only':'securities','securities.columns':'SECID,BOARDID,SHORTNAME,STATUS'}
df = pd.DataFrame(requests.get(url, params=params, timeout=20).json()['securities']['data'],
                  columns=requests.get(url, params=params, timeout=20).json()['securities']['columns'])

# 4) Фильтр по Si и рабочим бордам
df = df[df['SECID'].str.match(r'^Si[A-Z]\d$', na=False)]  # формат SiZ5, SiH6 и т.п.
# Приоритет борда RFUD/RFUT
# Если есть несколько записей по одному SECID, оставим RFUD в приоритете
df = df.sort_values(['SECID', 'BOARDID'])
df = df.drop_duplicates(subset=['SECID'], keep='first')

# 5) Распарсим год/месяц из кода
month_map = dict(H=3, M=6, U=9, Z=12)
def parse_code(secid):
    m = re.match(r'^Si([HMUZ])(\d)$', secid)
    if not m: return None
    mon = month_map[m.group(1)]
    yr = 2020 + int(m.group(2))  # десятилетие 2020s
    return yr, mon

df['YEAR_MONTH'] = df['SECID'].apply(parse_code)
df = df.dropna(subset=['YEAR_MONTH'])
df[['YEAR','MONTH']] = pd.DataFrame(df['YEAR_MONTH'].tolist(), index=df.index)

# 6) Выберем текущий/следующий контракт относительно сегодняшней даты
today = pd.Timestamp.today().normalize()
df['EXPIR'] = pd.to_datetime(dict(year=df['YEAR'], month=df['MONTH'], day=15))  # ориентировочно середина месяца экспирации
candidates = df[df['EXPIR'] >= today].sort_values(['YEAR','MONTH'])
if candidates.empty:
    # fallback: возьмём последний доступный
    chosen = df.sort_values(['YEAR','MONTH']).iloc[-1]['SECID']
else:
    chosen = candidates.iloc[0]['SECID']

print('Выбран контракт:', chosen)

# 7) Загрузка FUTOI по выбранному контракту
t = Ticker(chosen)
futoi = t.futoi(start=start, end=end)

print('Rows:', len(futoi), 'Columns:', list(futoi.columns))
print(futoi.tail(5))

# 8) Сохранение
out = pathlib.Path('data'); out.mkdir(exist_ok=True)
futoi.to_csv(out / f'futoi_{chosen}_{start}_{end}.csv', index=False)
print('Saved:', out / f'futoi_{chosen}_{start}_{end}.csv')
PY

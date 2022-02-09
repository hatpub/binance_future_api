import pymysql
from datetime import datetime, timedelta
import requests
import json
from apscheduler.schedulers.blocking import BlockingScheduler

def exec_cron():

    db = pymysql.connect(
        host='192.168.0.112',
        user='sslee',
        password='rhddbtjqj',
        charset='utf8mb4',
        database='sslee_DB'
    )

    url = 'https://fapi.binance.com/fapi/v1/ticker/24hr'
    res = requests.get(url)

    if res.status_code == 200:
        result_dict = json.loads(res.text)
    else:
        print(res.status.code)

    now = datetime.now()
    fd = now.strftime('%Y-%m-%d %H:%M:%S')
    curs = db.cursor()
    for rd in result_dict:
        if 'BTC' in rd['symbol'] and float(rd['lastPrice']) > 0:
            #curs = db.cursor()
            sql = """insert into fbase_btc(symbol, dt, price)
                    values (%s, %s, %s)"""
            curs.execute(sql, (rd['symbol'], fd, float(rd['lastPrice'])))
        else:
            continue
    db.commit()
    db.close()

sched = BlockingScheduler()
sched.add_job(exec_cron, 'cron', second='*/5')

sched.start()

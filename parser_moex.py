import json
import sys
import time
from datetime import datetime

import pandas as pd
import requests as re
from io import StringIO
import sqlite3
import pymysql.cursors
from future.moves import collections


def get_json_from_mysql():
    try:
        # conn = sqlite3.connect("parserdb.db")
        # cursor = conn.cursor()

        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='12345678',
            db='parserdb',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM parserdb.dateparse ORDER BY id DESC;")
        cur_cur = cursor.fetchall()

        objects_list = []

        for row in cur_cur:

            d = collections.OrderedDict()
            d['id'] = row['id']
            d['col_a'] = row['col_a']
            d['col_b'] = row['col_b']
            d['fiz_long'] = row['fiz_long']
            d['fiz_short'] = row['fiz_short']
            d['yur_long'] = row['yur_long']
            d['yur_short'] = row['yur_short']
            d['systime'] = row['systime']
            objects_list.append(d)

        js = json.dumps(objects_list, default=str)
        with open('json_file.js', 'w') as f:
            f.write(js)

        conn.commit()
    except pymysql.Error as error:  # sqlite3.Error as error:
        # pass
        print("ERROR ", error)

    finally:
        conn.close()



def connect_and_getData():
    s = re.Session()
    try:
        url_auth = 'https://passport.moex.com/authenticate'
        auth = ('rg174@mail.ru', 'Cnfhsq47')
        s.get(url=url_auth, auth=auth)
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                 "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"}
        cookies = {'MicexPassportCert': s.cookies['MicexPassportCert']}
        # url = 'https://iss.moex.com/iss/analyticalproducts/futoi/securities.csv'
        url = 'https://iss.moex.com/iss/analyticalproducts/futoi/securities/RI.csv'
        req = re.get(url=url, headers=headers, cookies=cookies)

        data = StringIO(req.text)
        pd.set_option('display.max_columns', None)
        usecols = ['sess_id', 'seqnum', 'ticker', 'clgroup', 'pos_long', 'pos_short', 'systime']
        read_csv = pd.read_csv(data, sep=';', skiprows=2, usecols=usecols).dropna()
        # vsystime = datetime.strptime(read_csv.iloc[0]['systime'], '%Y-%m-%d %H:%M:%S')
        vsystime = read_csv.iloc[0]['systime']
        data = read_csv.iloc[0:2, 3:6]

        fiz = data[data['clgroup'] == 'FIZ']
        yur = data[data['clgroup'] == 'YUR']

        print(f"-----{int(fiz['pos_long'])}  {int(fiz['pos_short'])}  {int(yur['pos_long'])}  {int(yur['pos_short'])}  {vsystime}")
        # print(vsystime)
        # print(datetime.strptime(vsystime, '%Y-%m-%d %H:%M:%S'))
        # return json.dumps(data_json)
        # pass
        try:
            # conn = sqlite3.connect("parserdb.db")
            # cursor = conn.cursor()

            conn = pymysql.connect(
                host='localhost',
                user='root',
                password='12345678',
                db='parserdb',
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor)

            cursor = conn.cursor()
            cursor.execute("SELECT * FROM parserdb.dateparse ORDER BY id DESC LIMIT 1;")
            # last_rec = cursor.execute("SHOW TABLES;")
            # last_rec_dict = last_rec.fetchall()

            col_a = 0
            col_b = 0
            # if last_rec_dict:
            if cursor.rowcount > 0:
                cur_cur = cursor.fetchall()[0]
                # print(last_rec_dict)
                last_fiz_long = cur_cur['fiz_long']  # last_rec_dict[0][3]
                last_fiz_short = cur_cur['fiz_short']  # last_rec_dict[0][4]
                last_yur_long = cur_cur['yur_long']  # last_rec_dict[0][5]
                last_yur_short = cur_cur['yur_short']  # last_rec_dict[0][6]
                if (int(last_fiz_long) != int(fiz['pos_long'])) \
                        or (int(last_fiz_short) != int(fiz['pos_short'])) \
                        or (int(last_yur_long) != int(yur['pos_long'])) \
                        or (int(last_yur_short) != int(yur['pos_short'])):
                    # col_a
                    # print(last_yur_long, last_yur_short, last_fiz_long, last_fiz_short)
                    if int(yur['pos_long']) > last_yur_long:
                        if int(fiz['pos_long']) < last_fiz_long:
                            col_a = 2 * (int(yur['pos_long']) - last_yur_long) / (last_yur_long - last_yur_short) * 1000
                            col_a = round(col_a, 2)
                            # print(f"------- col_a = {col_a}")
                    # col_b
                    #
                    if abs(int(yur['pos_short'])) > abs(last_yur_short):
                        if abs(int(fiz['pos_short'])) < abs(last_fiz_short):
                            col_b = 2 * (abs(int(yur['pos_short'])) - abs(last_yur_short)) / abs(
                                last_yur_long - last_yur_short) * 1000
                            col_b = round(col_b, 2)
                            # print(f"------- col_b = {col_b}")

                    cursor.execute("INSERT IGNORE INTO parserdb.dateparse"
                                   "(`col_a`,`col_b`,`fiz_long`,`fiz_short`"
                                   ",`yur_long`,`yur_short`,`systime`)"
                                   f"VALUES({float(col_a)},{float(col_b)},{int(fiz['pos_long'])}"
                                   f",{int(fiz['pos_short'])},{int(yur['pos_long'])}"
                                   f",{int(yur['pos_short'])},date_format('{vsystime}','%Y-%m-%d %H:%i'))")
                # print('OK1')
            else:
                # print('OK2')
                cursor.execute("INSERT IGNORE INTO parserdb.dateparse"
                               "(`col_a`,`col_b`,`fiz_long`,`fiz_short`"
                               ",`yur_long`,`yur_short`,`systime`)"
                               f"VALUES({float(col_a)},{float(col_b)},{int(fiz['pos_long'])}"
                               f",{int(fiz['pos_short'])},{int(yur['pos_long'])}"
                               f",{int(yur['pos_short'])},date_format('{vsystime}','%Y-%m-%d %H:%i'))")


            conn.commit()
        except pymysql.Error as error:  # sqlite3.Error as error:
            # pass
            print("ERROR ", error)

        finally:
            conn.close()
    except Exception as err:
        # pass
        print("ERROR ", error)
        # return []

def clear_base():
    try:
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='12345678',
            db='parserdb',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

        cursor = conn.cursor()
        cursor.execute("DELETE FROM parserdb.dateparse")
        conn.commit()
    except pymysql.Error as error:
        print("ERROR ", error)

from datetime import time as timeForExit

if __name__ == "__main__":
    time_exit = timeForExit(20, 33)
    time_clear_base = timeForExit(23, 55)

    print(f"{time_exit}         {time_clear_base}     {datetime.now().weekday()}")

    while True:
        connect_and_getData()
        time.sleep(1)
        get_json_from_mysql()
        time_now = datetime.now().time()
        day_week_now = datetime.now().weekday()
        if day_week_now == 6:
            if time_now >= time_clear_base:
                clear_base()
        if time_now >= time_exit:
            sys.exit(0)

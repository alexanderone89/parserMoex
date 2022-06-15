import asyncio
import sqlite3

import pymysql
from aiogram import Bot, Dispatcher, executor, types
from aiogram.dispatcher.filters import Text
from aiogram.utils.markdown import hbold, hlink
import time


Token = '5150401272:AAEDch_K3VG1XuPJYvN4KWvP7F5iiI0fAkA'
chat_id = '580295219'
bot = Bot(token=Token, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot)

titles = ['№', 'Значение A', 'Значение B', 'Длинные поз-ии физ лиц',
          'Короткие поз-ии физ лиц', 'Длинные поз-ии юр лиц',
          'Короткие поз-ии юр лиц', 'Время рассчета']


# приветствие для новых членов бота
# @dp.message_handler(content_types=["new_chat_members"])
# def handler_new_member(message):
#     user_name = message.new_chat_member.first_name
#     bot.send_message(message.chat.id, "Добро пожаловать, {0}!".format(user_name))

# код заставит бота моментально ответить пользователю той же гифкой, что была прислана
# @dp.message_handler(content_types=[types.ContentType.ANIMATION])
# async def echo_document(message: types.Message):
#     await message.reply_animation(message.animation.file_id)


@dp.message_handler(commands='start')
async def start(message: types.Message):
    start_buttons = ['Вывести все данные', 'Последние данные']#, 'Снайперские винтовки']
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(*start_buttons)

    await message.answer('<b>Привет!</b> Это специльный бот, созданный для ' \
                         ' отслеживания изменений на Московской фондовой бирже по некоторым позициям...\n' \
                         '<b>Внимание!</b> Бот выводит последние данные автоматически.',
    reply_markup=keyboard, parse_mode="HTML")


@dp.message_handler(Text(equals='Вывести все данные'))
async def get_all_records(message: types.Message):
    await message.answer('Пожалуйста подождите...')
    try:
        # conn = sqlite3.connect("parserdb.db")
        # cursor = conn.cursor()
        # res = cursor.execute("SELECT * FROM dateparse ORDER BY id DESC").fetchall()

        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='12345678',
            db='parserdb',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()
        # res = cursor.execute("SELECT * FROM dateparse ORDER BY id DESC").fetchall()
        cursor.execute("SELECT * FROM dateparse ORDER BY id DESC")

        res = cursor.fetchall()

        for row_number, row in enumerate(res):
            stroka = f"{titles[0]} : {hbold(str(row['id']))}\n" \
                     f"{titles[1]} : {hbold(str(row['col_a']))}\n" \
                     f"{titles[2]} : {hbold(str(row['col_b']))}\n" \
                     f"{titles[3]} : {hbold(str(row['fiz_long']))}\n" \
                     f"{titles[4]} : {hbold(str(row['fiz_short']))}\n" \
                     f"{titles[5]} : {hbold(str(row['yur_long']))}\n" \
                     f"{titles[6]} : {hbold(str(row['yur_short']))}\n" \
                     f"{titles[7]} : {hbold(str(row['systime']))}"

        # for row_number, row in enumerate(res):
        #     stroka = ''
        #     for column_number, data in enumerate(row):
        #         stroka += f'{titles[column_number]}: {hbold(data)}\n'
        #     if row_number % 20 == 0:
        #         time.sleep(3)

            await message.answer(stroka)

        # self.data_signal.emit(self._list)  # отдаем список в основной поток
    except sqlite3.Error as error:
        print("ERROR ", error)

    finally:
        conn.close()


@dp.message_handler(Text(equals='Последние данные'))
async def get_last_record(message: types.Message):
    await message.answer('Пожалуйста подождите...')

    try:
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='12345678',
            db='parserdb',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dateparse ORDER BY id DESC LIMIT 1")
        last_rec = cursor.fetchall()[0]
        # print(last_rec)
        stroka = ''
        stroka = f"{titles[0]} : {hbold(str(last_rec['id']))}\n" \
                 f"{titles[1]} : {hbold(str(last_rec['col_a']))}\n" \
                 f"{titles[2]} : {hbold(str(last_rec['col_b']))}\n" \
                 f"{titles[3]} : {hbold(str(last_rec['fiz_long']))}\n" \
                 f"{titles[4]} : {hbold(str(last_rec['fiz_short']))}\n" \
                 f"{titles[5]} : {hbold(str(last_rec['yur_long']))}\n" \
                 f"{titles[6]} : {hbold(str(last_rec['yur_short']))}\n" \
                 f"{titles[7]} : {hbold(str(last_rec['systime']))}"

        await message.answer(stroka)
        conn.commit()

    except pymysql.Error as error:
        print("ERROR ", error)

    finally:
        conn.close()


def get_count_from_base():
    try:
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='12345678',
            db='parserdb',
            charset='utf8mb4',)
        # conn = pymysql.connect(
        #     host='localhost',
        #     user='a0662123_parserdb',
        #     password='Accountforparse2000',
        #     db='a0662123_parserdb',
        #     charset='utf8mb4')
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dateparse")

        pcount = cursor.fetchall()[0][0]
        # print(pcount)
        return pcount

    except pymysql.Error as error:
        print("ERROR ", error)

    finally:
        conn.close()


# @dp.message_handler()
async def send_last_record():

    try:
        while True:
            conn = pymysql.connect(
                host='localhost',
                user='root',
                password='12345678',
                db='parserdb',
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor)
            # conn = pymysql.connect(
            #     host='localhost',
            #     user='a0662123_parserdb',
            #     password='Accountforparse2000',
            #     db='a0662123_parserdb',
            #     charset='utf8mb4')
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM dateparse")
            table_count = cursor.fetchall()[0].values()[0]
            print(table_count)
            # await bot.send_message(chat_id=chat_id, text=table_count)

            # cursor.execute("SELECT * FROM dateparse ORDER BY id DESC LIMIT 1")
            # last_rec = cursor.fetchall()[0]
            # # print(last_rec)
            # # if (last_rec['col_a'] != 0) or (last_rec['col_b']):
            # stroka = f"{titles[0]} : {hbold(str(last_rec['id']))}\n" \
            #          f"{titles[1]} : {hbold(str(last_rec['col_a']))}\n" \
            #          f"{titles[2]} : {hbold(str(last_rec['col_b']))}\n" \
            #          f"{titles[3]} : {hbold(str(last_rec['fiz_long']))}\n" \
            #          f"{titles[4]} : {hbold(str(last_rec['fiz_short']))}\n" \
            #          f"{titles[5]} : {hbold(str(last_rec['yur_long']))}\n" \
            #          f"{titles[6]} : {hbold(str(last_rec['yur_short']))}\n" \
            #          f"{titles[7]} : {hbold(str(last_rec['systime']))}"
            # # print(stroka)
            # await bot.send_message(chat_id=chat_id, text=stroka)

    except pymysql.Error as error:
        print("ERROR ", error)

    finally:
        conn.close()
    # time.sleep(3)


def main():

    loop = asyncio.get_event_loop()
    loop.create_task(send_last_record())
    # dp.loop.create_task(send_last_record())
    executor.start_polling(dp, skip_updates=True)
    print('OK')


if __name__ == '__main__':
    main()

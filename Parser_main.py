import datetime
import sys
import os
import time
import yagmail
from multiprocessing.dummy import Pool as ThreadPool
import random

import parser_utils
import yarregion_notifs
import yarregion_contracts

"""""
-- Author: STEEL
-- Create date: 21.10.2021
-- Description:	Парсинг сайта https://yarregion.ru 
-- Раздел закупки: https://yarregion.ru/request/public-requests

"""""

sql_login_file = 'login_sql.txt'
mail_login_file = 'login_mail.txt'
mails_file = 'mails_to_send.txt'
words_file = 'words.txt'

Debug = False

# Создание дирректории для логов
startTime = datetime.datetime.now()
MLog = startTime.strftime('%m')
YLog = startTime.strftime('%Y')

if os.path.exists('./src/Logs/' + str(YLog)) == False:
    if os.path.exists('./src/Logs/') == False:
        if os.path.exists('./src/') == False:
            os.mkdir('./src/')
        os.mkdir('./src/Logs/')
    os.mkdir('./src/Logs/' + str(YLog))

# Запись в лог
def InsertLog (StrLog):
    startTime = datetime.datetime.now()
    t_log = startTime.strftime('%d.%m.%Y %H:%M:%S')
    log = open('./src/Logs/' + str(YLog) + '/Logs_' + str(MLog) + '.txt', 'a+')
    log.write(t_log + '  # ' + StrLog + '\n')
    log.close()

if __name__ == '__main__':
    # Приветствие
    DT = datetime.datetime.today().weekday()
    print('It`s ' + str(datetime.datetime.today().strftime('%A')))

    startT = datetime.datetime.now()  # Время для отслеживания работы всего парсера
    startTime = datetime.datetime.now()  # Забираем сегодняшнюю дату из открытой библитеки
    t1 = startTime.strftime('%Y-%m-%d')  # Форматирование даты №1
    t2 = startTime.strftime('%Y%m%d')
    yesterdayTime = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterdayTime.strftime('%Y%m%d')

    print('Start at:  ' + str(startT))

    login_sql, password_sql = parser_utils.login(sql_login_file)
    login_mail, password_mail = parser_utils.login(mail_login_file)

    mails = parser_utils.get_list_from_txt(mails_file)
    words_to_parse = parser_utils.get_list_from_txt(words_file)

    # Запись в лог
    InsertLog('********** НАЧАЛО **********')

    if len(sys.argv) > 1:  # Достаем номера через консоль (если арги были даны)
        if str(sys.argv[1]).lower() == 'n':  # Если извещения
            word = None
            args = []
            i = 2
            ln = len(sys.argv)
            while i < ln:
                arguments = sys.argv[i]
                args.append(arguments)
                i += 1
            yarregion_notifs.parse_notifs(login_sql, password_sql, word, t1, isArgs=True, args_to_parse=args, isDebug=Debug)
        elif str(sys.argv[1]).lower() == 'c':  # Если контракты
            word = None
            args = []
            i = 2
            ln = len(sys.argv)
            while i < ln:
                arguments = sys.argv[i]
                args.append(arguments)
                i += 1
            yarregion_contracts.parse_contracts(login_sql, password_sql, word, t1, isArgs=True, args_to_parse=args, isDebug=Debug)
        elif len(sys.argv[1]) == 8:  # если длина похожа на YYYYMMDD, то парсим по дате
            date_start = sys.argv[1]
            for word in words_to_parse:
                word = word.replace('?', '')
                yarregion_notifs.parse_notifs(login_sql, password_sql, word, date_start, isDebug=Debug)
                yarregion_contracts.parse_contracts(login_sql, password_sql, word, date_start, isDebug=Debug)
        else:
            word = None
            args = []
            i = 1
            ln = len(sys.argv)
            while i < ln:
                arguments = sys.argv[i]
                args.append(arguments)
                i += 1
            yarregion_notifs.parse_notifs(login_sql, password_sql, word, t1, isArgs=True, args_to_parse=args, isDebug=Debug)
    else:  # Достаем номера через поиск
        for word in words_to_parse:
            word = word.replace('?', '')
            word = word.replace('﻿','')

            #yarregion_contracts.parse_contracts(login_sql, password_sql, isDebug=Debug)
            #sys.exit()

            #MNN_TN_corrector.correct(login_sql, password_sql, yesterday, isDebug=Debug)
            #sys.exit()

            yarregion_notifs.parse_notifs(login_sql, password_sql, word, t1, isDebug=Debug)
            #sys.exit()

    #sys.exit()

    # Запись в лог
    InsertLog('Загрузка протоколов и обновление статусов извещений НАЧАЛО')

    #STEEL Контрактов на сайте нет. Извещения для загрузки протоколов и проверки статусов берутся из базы: spyarregion_getTenderForContract
    yarregion_contracts.parse_contracts(login_sql, password_sql, isDebug=Debug)

    # Запись в лог
    InsertLog('Загрузка протоколов и обновление статусов извещений КОНЕЦ')

    # Отправка статистики и отчетов
    end = str(datetime.datetime.now() - startT)
    statistics_query = "SELECT [id] FROM [CursorImport].[log].[yarregion_status] where DTCreate >= '" + t2 + "'"
    stats_list = parser_utils.select_query(statistics_query, login_sql, password_sql, isList=True)

    contents_st = ['Парсинг сайта yarregion завершен. Всего времени на выполнение: ' + end + '\n\nВсего строк: ' + str(len(stats_list))]
    print('Sending mail ...')
    yag = yagmail.SMTP(login_mail, password_mail, host='smtp.yandex.ru')
    yag.send(mails, 'Завершение парсинга сайта Ярославской обл. https://zakaz.yarregion.ru', contents_st)

    # Запись в лог
    InsertLog('Отправили статистику и отчеты')

    print('Всего времени на выполнение: ' + end)
    InsertLog('Всего времени на выполнение: ' + end)
    print('End at:  ' + str(datetime.datetime.now()))
    print('All Done!')


    # Запись в лог
    InsertLog('********** КОНЕЦ **********')

    time.sleep(3)
    sys.exit()


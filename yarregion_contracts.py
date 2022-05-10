import requests
#import pandas as pd
import pyodbc
#import sys
import time
import datetime
#import math
import random
#import json
import traceback
from termcolor import colored
import urllib3
#import re
#from dateutil.parser import parse
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import parser_utils
# openpyxl

#STEEL от 27.12.2021 получаем список извещений для проверки победителей. Контрактов на сайте нет

startTime = datetime.datetime.now()
MLog = startTime.strftime('%m')
YLog = startTime.strftime('%Y')

# Запись в лог
def InsertLog (StrLog, NoPrint = 0):
    if NoPrint == 0:
        print(StrLog)
    startTime = datetime.datetime.now()
    t_log = startTime.strftime('%d.%m.%Y %H:%M:%S')
    log = open('./src/Logs/' + str(YLog) + '/Logs_' + str(MLog) + '.txt', 'a+')
    log.write(t_log + '  # ' + StrLog + '\n')
    log.close()

# Для смены UserAgent
def GetRandomUserAgent ():
    with open("UserAgent.txt") as file:
        UserAgentAr = [row.strip() for row in file]

    Rand = random.randint(1, len(UserAgentAr))
    if Rand == len(UserAgentAr):
        Rand = Rand - 1
    return UserAgentAr[Rand]

def is_number(str):
    try:
        float(str)
        return True
    except:
        return False

def is_int(str):
    try:
        int(str)
        return True
    except:
        return False

def parse_contracts(login_sql, password_sql, isArgs=False, args_to_parse=None, isDebug=False):
    # Добываем прокси из базы
    # df_proxy = parser_utils.get_proxies(login_sql, password_sql)

    needproxy = False
    isNotDone = True

    for i in range(0, 100):
        while isNotDone:
            try:
                #proxy = df_proxy.sample(1)['proxy'].reset_index(drop=True)[0]  # Берем один рандомный прокси из DataFrame
                print('Попытка №' + str(i + 1))

                empty = False  # Проверка на пустой запрос по слову
                collected = False  # Проверка на готовность данных
                isError = False

                try:
                    if isArgs == False:
                        #STEEL от 27.12.2021 получаем список извещений для проверки победителей. Контрактов на сайте нет
                        tender_query = "exec [dbo].[spYarregion_getTenderForContract]"
                        tender_list = parser_utils.select_query(tender_query, login_sql, password_sql, isList=False)

                        total_args = len(tender_list)
                        print(colored('Всего извещений для загрузки протоколов: ' + str(total_args), 'blue'))
                        # Запись в лог
                        InsertLog('Всего извещений для загрузки протоколов: ' + str(total_args), 1)

                        num0 = 0
                    else:
                        if type(args_to_parse) == 'str' or type(args_to_parse) == 'int':
                            total_args = len([args_to_parse])
                        else:
                            total_args = len(args_to_parse)

                        num0 = 0

                    while num0 < total_args:
                        empty = False

                        if isArgs == False:

                            NotifNr = tender_list['NotifNr'][num0]
                            LotStatus = tender_list['LotStatus'][num0]
                            SrcInf = tender_list['SrcInf'][num0]
                            paramPurchase = tender_list['paramPurchase'][num0]
                            url_winner = "https://zakupki.yarregion.ru/zakupki-malogo-obema-elektronnogo-magazina-detalno/redirect/localhost/Data?uuid=17199e28-6005-4dcd-9fb0-8381d5a0400c&dataVersion=30.04.2021 07.11.35.278&dsCode=gridClsData&paramPurchase=" + paramPurchase + "&purchaseNumParam=" + NotifNr + "&isUndefinedValue=Нет&_dc=1640606642270"
                            url_status = "https://zakupki.yarregion.ru/zakupki-malogo-obema-elektronnogo-magazina-detalno/redirect/localhost/Data?uuid=17199e28-6005-4dcd-9fb0-8381d5a0400c&dataVersion=30.04.2021 07.11.35.278&dsCode=OOS_RKS_01_02_infoData&paramPurchase=" + paramPurchase + "&_dc=1640614084499"

                            # Запись в лог
                            InsertLog(NotifNr + ' ' + SrcInf)
                            #sys.exit()
                        else:
                            y1 = str(args_to_parse[num0]).replace("'", '').replace("(", '').replace(")", '').replace(
                                ",", '').replace(" ", '')

                            tender_query = "exec [CursorImport].[dbo].[spSberb2b_getTenderForContract] '" + y1 + "'"
                            tender_list = parser_utils.select_query(tender_query, login_sql, password_sql, isList=False)
                            NotifNr = tender_list['NotifNr'][num0]
                            LotStatus = tender_list['LotStatus'][num0]

                            url_full = "https://sberb2b.ru/request/get-public-requests?r_published_at=desc"

                        UserAgent = GetRandomUserAgent()

                        headers = {'Accept': '*/*',
                                   'Accept-Encoding': 'gzip, deflate, br',
                                   'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
                                   'Connection': 'keep-alive',
                                   'host': 'zakupki.yarregion.ru',
                                   'origin': 'https://zakupki.yarregion.ru',
                                   'Sec-Fetch-Dest': 'empty',
                                   'Sec-Fetch-Mode': 'cors',
                                   'Sec-Fetch-Site': 'same-origin',
                                   'TE': 'trailers',
                                   'user-agent': UserAgent,
                                   'x-requested-with': 'XMLHttpRequest'}

                        if isArgs == False:

                            if needproxy == True:
                                try:
                                    r00 = requests.post(url_winner, headers=headers, verify=False,timeout=50)
                                except requests.exceptions.Timeout as e:
                                    print('Connection timeout')
                                    isError = True
                            else:
                                try:
                                    r00 = requests.post(url_winner, headers=headers, verify=False, timeout=50)
                                except requests.exceptions.Timeout as e:
                                    print('Connection timeout')
                                    isError = True

                            InsertLog(str(r00.status_code) + '. Проверка победителя. Достаем json')

                            if r00.status_code != 200:
                                isError = True

                            r00 = r00.json()

                            AllInfo = []
                            data_id = 0
                            leng = 0

                            if r00['data'] != None:
                                leng = len(r00['data'])
                                if leng > 3:
                                    leng = 3

                            #print(leng)
                            #print(r00['data'])

                            # Проставить победителя
                            while data_id < leng:
                                Win_Num = int(r00['data'][data_id][0])
                                Win_Name = r00['data'][data_id][1]
                                Win_INN = r00['data'][data_id][2]
                                Win_KPP = r00['data'][data_id][3]
                                Win_Adr = r00['data'][data_id][4]
                                Win_Tel = r00['data'][data_id][5]
                                Win_EMail = r00['data'][data_id][6]
                                Win_Price = r00['data'][data_id][7]
                                Win_Date = r00['data'][data_id][8]

                                #AllInfo.append([Win_Num, Win_Name, Win_INN, Win_KPP, Win_Adr, Win_Tel, Win_EMail, Win_Price, Win_Date])

                                #data_id += 1

                                if Win_KPP == None:
                                    Win_KPP = ''
                                if Win_Adr == None:
                                    Win_Adr = ''

                                #print(Win_Num, Win_Name, Win_INN, Win_KPP, Win_Adr, Win_Tel, Win_EMail, Win_Price, Win_Date)
                                #sys.exit()

                                if Win_Num != None and Win_Name != None and Win_INN != None and Win_Price != None:
                                    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                                          'Server=37.203.243.65\CURSORMAIN,49174;'
                                                          'Database=Cursor;'
                                                          'UID=' + login_sql + ';'
                                                          'PWD=' + password_sql + ';'
                                                          'Trusted_Connection=no;')
                                    cursor = conn.cursor()
                                    cursor.execute("exec [Cursor].[dbo].[spYarregion_ProtoStats] '" + NotifNr + "', " + str(Win_Num) + ", " + str(Win_Price) + ", '" + Win_INN + "', '" + Win_KPP + "', '" + Win_Name + "', '" + Win_Adr + "'")

                                    if isDebug == False:
                                        conn.commit()
                                    conn.close()

                                    print(colored('Проставили протокол. Участник ' + str(Win_Num) + ' (' + Win_Name + ').', 'blue'))

                                    # Запись в лог
                                    InsertLog('Проставили протокол. Участник ' + str(Win_Num) + ' (' + Win_Name + ').', 1)
                                else:
                                    # Запись в лог
                                    InsertLog('Ошибка. Не все ключевые поля заполненны: Win_Num="' + str(Win_Num) + '", Win_Name="' + Win_Name + '", Win_INN=' + Win_INN + '", Win_Price=' + Win_Price + '"')

                                data_id += 1

                        else:
                            if type(args_to_parse) == 'str' or type(args_to_parse) == 'int':
                                total_args = len([args_to_parse])
                            else:
                                total_args = len(args_to_parse)

                            num0 = 0

                        # Победителя нет. Проверить статус.
                        if leng == 0:
                            if needproxy == True:
                                try:
                                    r00 = requests.post(url_status, headers=headers, verify=False,timeout=50)
                                except requests.exceptions.Timeout as e:
                                    print('Connection timeout')
                                    isError = True
                            else:
                                try:
                                    r00 = requests.post(url_status, headers=headers, verify=False, timeout=50)
                                except requests.exceptions.Timeout as e:
                                    print('Connection timeout')
                                    isError = True

                            InsertLog(str(r00.status_code) + '. Победителя нет. Проверка статуса. Достаем json')

                            if r00.status_code != 200:
                                isError = True

                            r00 = r00.json()

                            # Состояние
                            r8_status = None

                            try:
                                r8_status = r00['data'][5][1]
                                r8_status = r8_status.replace('Завершен', '4').replace('Отменен', '5').replace('Опубликован', '7')
                            except:
                                InsertLog('## Ошибка при разборе статуса извещения')

                            # Обновить статус извещения
                            if r8_status != None and ('4' in r8_status or '7' in r8_status or '5' in r8_status):
                                    if int(r8_status) != LotStatus:
                                            conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                                                  'Server=37.203.243.65\CURSORMAIN,49174;'
                                                                  'Database=CursorImport;'
                                                                  'UID=' + login_sql + ';'
                                                                  'PWD=' + password_sql + ';'
                                                                  'Trusted_Connection=no;')
                                            cursor = conn.cursor()
                                            cursor.execute("exec [CursorImport].[dbo].[spYarregion_TenderUpdStatus] '" + NotifNr + "', " + str(r8_status))

                                            if isDebug == False:
                                                conn.commit()
                                            conn.close()
                                            print(colored('Обновили статус (' + str(r8_status) + ') у извещения: ' + NotifNr, 'blue'))

                                            # Запись в лог
                                            InsertLog('Обновили статус (' + str(r8_status) + ') у извещения: ' + NotifNr, 1)
                                    else:
                                            conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                                                  'Server=37.203.243.65\CURSORMAIN,49174;'
                                                                  'Database=CursorImport;'
                                                                  'UID=' + login_sql + ';'
                                                                  'PWD=' + password_sql + ';'
                                                                  'Trusted_Connection=no;')
                                            cursor = conn.cursor()
                                            cursor.execute("update log.yarregion_status set DTCreate=getdate() where NotifNr= '" + NotifNr + "'")

                                            if isDebug == False:
                                                conn.commit()
                                            conn.close()
                                            print(colored('Cтатус не изменился (' + str(r8_status) + ') у извещения: ' + NotifNr, 'blue'))

                                            # Запись в лог
                                            InsertLog('Cтатус не изменился (' + str(r8_status) + ') у извещения: ' + NotifNr, 1)

                        num0 += 1
                    else:
                        print(colored('Загрузка протоколов завершена', 'blue'))

                        # Запись в лог
                        InsertLog('Загрузка протоколов завершена', 1)

                        collected = True
                        time.sleep(2)
                        isNotDone = False
                except:
                    if empty == True and collected == True:
                        InsertLog('Нет данных по этому слову')
                        isNotDone = False
                    elif empty == False and collected == True:
                        InsertLog('Все данные собраны')
                        isNotDone = False
                    elif isError == True:
                        InsertLog('Подключение не удалось')
                        pass
                    else:
                        #if isDebug == True:
                        #    traceback.print_exc()
                        traceback.print_exc()

                        isNotDone = False
                        # Запись в лог
                        startTime = datetime.datetime.now()
                        t_log = startTime.strftime('%d.%m.%Y %H:%M:%S')
                        log = open('./src/Logs/' + str(YLog) + '/Logs_' + str(MLog) + '.txt', 'a+')
                        log.write(t_log + ' ## Подключение не удалось (ДРУГАЯ ОШИБКА)' + '\n')
                        traceback.print_exc(file=log)
                        log.close()

                    pass

                #num0 += 1
            except:
                isNotDone = False
                pass
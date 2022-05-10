import requests
#from bs4 import BeautifulSoup
import pandas as pd
import pyodbc
import time
import datetime
from dateutil.parser import parse
#import math
#import json
#import re
import random
import traceback
from termcolor import colored
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import parser_utils
import Org_creator
# openpyxl

columns_pos = ['Наименование товара (работ, услуг)', 'Количество', 'Единица измерения', 'Извещение', 'наим изв', 'окпд', 'цена', 'сумма', 'форма']
columns_not = ['Номер извещения', 'Наименование', 'Заказчик', 'Планируемая дата исполнения договора',
                 'Количество позиций', 'Стартовая цена, руб.',
                 'Место доставки (выполнения работ, оказания услуг) или указание на самовывоз', 'Статус закупки',
                 'Ссылка на позиции', 'Подведены итоги', 'Наименование Орга', 'ИНН', 'КПП', 'Адрес', 'ФИО', 'почта',
                 'телефон', 'Способ закупки', 'Наименование закупки', 'Тип', 'Вид оплаты', 'Условия оплаты',
                 'Календарных дней', 'Стартовая цена', 'Размещение закупки', 'Окончание подачи заявок',
                 'Дата заключения договора', 'Регион поставки', 'Самовывоз', 'Макс срок поставки', 'Status', 'HaveDocs']

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

def parse_notifs(login_sql, password_sql, word, date, isArgs=False, args_to_parse=None, isDebug=False):
    if word != None:
        print(colored(word.upper(), 'green'))

    needproxy = False

    # Добываем прокси из базы
    if needproxy == True:
        df_proxy = parser_utils.get_proxies(login_sql, password_sql)

    # Запись в лог
    InsertLog('********** НАЧАЛО yarregion_notifs.parse_notifs. СЛОВО: ' + str(word))

    m_izv_name = []
    m_link = []
    m_pos_name = []
    m_notifnr = []
    m_pos_form = []

    isNotDone = True
    for i in range(0, 10):
        while isNotDone:
            try:
                #print('Попытка №' + str(i + 1))

                # Запись в лог
                InsertLog('Попытка №' + str(i + 1))


                if needproxy == True:
                    proxy = df_proxy.sample(1)['proxy'].reset_index(drop=True)[0]  # Берем один рандомный прокси из DataFrame
                    #print('Подключаюсь, используя прокси: ' + str(proxy))

                    # Запись в лог
                    InsertLog('Подключаюсь, используя прокси: ' + str(proxy))

                    prox = str(proxy).replace(' ', '')
                    proxies = {'http': 'http://' + prox,
                               'https': 'http://' + prox,
                               }

                empty = False  # Проверка на пустой запрос по слову
                collected = False  # Проверка на готовность данных
                isError = False
                r00 = None

                try:
                    # Сборка url
                    if isArgs == False:
                        Dtime_Start = '2022-01-01T00:00:00.000Z'
                        CurrentDateT = datetime.datetime.now()
                        CurrentDate = CurrentDateT.strftime('%Y-%m-%dT00:00:00.000Z')
                        url_ful0 = 'https://zakupki.yarregion.ru/purchasesoflowvolume-asp/redirect/localhost/Data?uuid=8fff1ff7-e1da-4150-bdd7-2d3fc0709559&dataVersion=28.04.2021 07.57.58.612&dsCode=OOS_RCS_001_001_cardData&paramCompareDate=' + Dtime_Start + '&paramCurrentDate='+CurrentDate+'&paramSearchObject=' + word + '&paramSearchCustomer=&minPrice=&maxPrice=&purchaseNumParam=&paramSearchOKPD=&paramStateGroup=031,032,038,006&paramRuleActGroup=ISZ_RCS,ISZ_223&paramUndefinedValueGroup=Да,Нет&_dc=1638344702188'


                    UserAgent = GetRandomUserAgent()

                    headers = {'Accept': '*/*',
                               'Accept-Encoding': 'gzip, deflate, br',
                               'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
                               'Connection': 'keep-alive',
                               'host': 'zakupki.yarregion.ru',
                               'Origin': 'https://zakupki.yarregion.ru',
                               'referer': 'https://zakupki.yarregion.ru/purchasesoflowvolume-asp/',
                               'Sec-Fetch-Dest': 'empty',
                               'Sec-Fetch-Mode': 'cors',
                               'Sec-Fetch-Site': 'same-origin',
                               'TE': 'trailers',
                               'user-agent': UserAgent,
                               'x-requested-with': 'XMLHttpRequest'}

                    if isArgs == False:

                        if needproxy == True:
                            try:
                                r00 = requests.post(url_ful0, headers=headers, verify=False, timeout=50)

                                #r00 = requests.post(url_ful0, data=json.dumps(payload), headers=headers, verify=False, timeout=50)  # Пробуем подключиться
                            except requests.exceptions.Timeout as e:
                                print('Connection timeout')
                                isError = True
                        else:
                            try:
                                r00 = requests.post(url_ful0, headers=headers, verify=False, timeout=50)
                            except requests.exceptions.Timeout as e:
                                print('Connection timeout')
                                isError = True

                        if r00.status_code != 200:
                           isError = True

                        # Достаем json
                        print(r00)

                        #sys.exit()

                        # Запись в лог
                        InsertLog('Достаем json с данными')

                        r00 = r00.json()

                        total = len(r00['data'])

                        print(colored('Всего извещений: ' + str(total), 'blue'))
                        #print(colored('Всего страниц: ' + str(total_args), 'blue'))

                        # Запись в лог
                        InsertLog('Всего извещений: ' + str(total), 1)
                        #InsertLog('Всего страниц: ' + str(total_args))

                        #print(r00['data'][1][0])
                        #sys.exit()

                        num0 = 0 # все извещения на одной странице
                    else:
                        if type(args_to_parse) == 'str' or type(args_to_parse) == 'int':
                            total_args = len([args_to_parse])
                        else:
                            total_args = len(args_to_parse)

                        num0 = 0


                    #Dtime_Start = '2020-01-01T00:00:00.000Z'
                    startTC = datetime.datetime.now()
                    CurrentDate = startTC.strftime('%Y-%m-%dT00:00:00.000Z')  # Текущая дата для запросов

                    # Если 10 повторов, то переходить к следующему слову для поиска.
                    ncount = 0

                    # Кол-во извещений для записи в базу. Если = 10, то будет записывать по 10 изв.
                    bdcount = 10
                    while num0 < total and (isArgs == True or (isArgs == False and ncount < 10)): # загружать с 2020 г.
                        # Сборка запроса
                        e1 = []
                        e2 = []
                        e3 = []
                        e4 = []
                        e5 = []
                        e6 = []
                        e7 = []
                        e8 = []
                        e9 = []
                        e10 = []
                        e11 = []
                        e12 = []
                        e13 = []
                        e14 = []
                        e15 = []
                        e16 = []
                        e17 = []
                        e18 = []
                        e19 = []
                        e20 = []
                        e21 = []
                        e22 = []
                        e23 = []
                        e24 = []
                        e25 = []
                        e26 = []
                        e27 = []
                        e28 = []
                        e29 = []
                        e30 = []
                        status = []
                        HaveDocs_mass = []

                        pos_name = []
                        pos_qty = []
                        pos_value = []
                        pos_notifnr = []
                        izv_name = []
                        pos_okpd = []
                        pos_price = []
                        pos_summ = []
                        pos_form = []

                        while bdcount != 0 and num0 < total and (isArgs == True or (isArgs == False and ncount < 200)):
                            Rnd = random.uniform(0.3, 0.8)
                            time.sleep(Rnd)

                            # Пример: [191802595, '01712000003202100220', 'Поставка лекарственных препаратов для медицинского применения на 2022 год (Хлоргексидин)', '01712000003202100220 (44–ФЗ)', 'ДЕПАРТАМЕНТ ЗДРАВООХРАНЕНИЯ И ФАРМАЦИИ ЯРОСЛАВСКОЙ ОБЛАСТИ', '2021-12-02T07:28:46.000Z', '2021-12-07T07:05:00.000Z', 'green', '150,00 ?', 'Нет', 'Опубликован', 'https://market.yarregion.ru/application/main?fid=17241&mid=191802595', 1182102276.81, 9013, '11:12:50', '', 'darkgreen', 'inherit', 1, '02.12.2021', '07.12.2021 10:05:00', '44–ФЗ', 32602.34]
                            ID_Notif = r00['data'][num0][0]
                            Num_Notif = r00['data'][num0][1]
                            Cust_Word = r00['data'][num0][4]
                            Cust_Word_Orig = r00['data'][num0][4]

                            # Ссылка для получения заказчика из реестра
                            url_cust_api = 'https://zakupki.yarregion.ru/reestr-postavshhikov/redirect/localhost/Data'

                            # Ссылка для получения заказчика из Карточки заказчика
                            url_cust_api2 = 'https://zakupki.yarregion.ru/kartochka-zakazchika/redirect/localhost/Data?uuid=405e6bdb-4d57-4fb7-90ef-a1316cc62b27&dataVersion=22.08.2019 14.06.02.253&dsCode=OOS_OOS_003_003_grbs_pbs_Data&beginPeriodParam=2021-01-01T00:00:00.000Z&endPeriodParamCurr=' + CurrentDate + '&_dc=1638869710881'

                            # Ссылка на извещение в базу
                            Url_Notif = 'https://zakupki.yarregion.ru/zakupki-malogo-obema-elektronnogo-magazina-detalno/?j&purchaseNumParam=' + str(Num_Notif) + '&paramPurchase=' + str(ID_Notif) +'&viewCode=View_cls'

                            # Сборка url для позиций извещения
                            url_notif_api = 'https://zakupki.yarregion.ru/zakupki-malogo-obema-elektronnogo-magazina-detalno/redirect/localhost/Data?uuid=17199e28-6005-4dcd-9fb0-8381d5a0400c&dataVersion=30.04.2021 07.11.35.278&dsCode=gridData&paramPurchase=' + str(ID_Notif) + '&_dc=1638354234456'

                            # Для Наименования извещения
                            url_notif_name = 'https://zakupki.yarregion.ru/zakupki-malogo-obema-elektronnogo-magazina-detalno/redirect/localhost/Data?uuid=17199e28-6005-4dcd-9fb0-8381d5a0400c&dataVersion=30.04.2021 07.11.35.278&dsCode=OOS_RKS_01_02_infoData&paramPurchase=' + str(ID_Notif) + '&_dc=1638857878568'

                            # Для документации
                            url_docs = 'https://zakupki.yarregion.ru/zakupki-malogo-obema-elektronnogo-magazina-detalno/redirect/localhost/Data?uuid=17199e28-6005-4dcd-9fb0-8381d5a0400c&dataVersion=30.04.2021 07.11.35.278&dsCode=documentData&purchaseNumParam=' + str(Num_Notif) + '&_dc=1638857878761'

                            # Запись в лог
                            InsertLog('Извещение ' + str(num0) + ' - ' + str(Num_Notif) + '. ID: ' + str(ID_Notif))

                            # Запись в лог
                            InsertLog(Url_Notif)

                            # В данных нет ИНН и КПП, поэтому получение ИНН/КПП заказчика из реестра.
                            InsertLog('Получение ИНН, КПП заказчика из реестра')

                            #Заменить сокращённые названия в заказчике, чтобы корректно прошёл поиск на сайте
                            Cust_Word = Cust_Word.replace('ГБУЗ ЯО "ЯОКГВВ - МЦ ','государственное бюджетное учреждение здравоохранения Ярославской области "Ярославский областной клинический госпиталь ветеранов войн - международный центр по проблемам пожилых людей ')
                            Cust_Word = Cust_Word.replace('ГКУ СО ЯО ГАВРИЛОВ-ЯМСКИЙ ДЕТСКИЙ ДОМ-ИНТЕРНАТ ДЛЯ УО ДЕТЕЙ', 'ГОСУДАРСТВЕННОЕ КАЗЕННОЕ УЧРЕЖДЕНИЕ СОЦИАЛЬНОГО ОБСЛУЖИВАНИЯ ЯРОСЛАВСКОЙ ОБЛАСТИ ГАВРИЛОВ-ЯМСКИЙ ДЕТСКИЙ ДОМ-ИНТЕРНАТ ДЛЯ УМСТВЕННО ОТСТАЛЫХ ДЕТЕЙ')
                            Cust_Word = Cust_Word.replace('ГУЗ ЯО','ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ ЗДРАВООХРАНЕНИЯ ЯРОСЛАВСКОЙ ОБЛАСТИ').replace(\
                                                          'ГБУЗ ЯО','ГОСУДАРСТВЕННОЕ БЮДЖЕТНОЕ УЧРЕЖДЕНИЕ ЗДРАВООХРАНЕНИЯ ЯРОСЛАВСКОЙ ОБЛАСТИ').replace( \
                                                          'ГКУЗ ЯО','ГОСУДАРСТВЕННОЕ КАЗЁННОЕ УЧРЕЖДЕНИЕ ЗДРАВООХРАНЕНИЯ ЯРОСЛАВСКОЙ ОБЛАСТИ').replace( \
                                                          'ГБКУЗ ЯО','ГОСУДАРСТВЕННОЕ БЮДЖЕТНОЕ КЛИНИЧЕСКОЕ УЧРЕЖДЕНИЕ ЗДРАВООХРАНЕНИЯ ЯРОСЛАВСКОЙ ОБЛАСТИ').replace(\
                                                          'ГПОУ ЯО', 'ГОСУДАРСТВЕННОЕ ПРОФЕССИОНАЛЬНОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ЯРОСЛАВСКОЙ ОБЛАСТИ').replace(\
                                                          'ГОУ ЯО','ГОСУДАРСТВЕННОЕ ОБЩЕОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ЯРОСЛАВСКОЙ ОБЛАСТИ').replace(\
                                                          'ГБУ ЯО','ГОСУДАРСТВЕННОЕ БЮДЖЕТНОЕ УЧРЕЖДЕНИЕ ЯРОСЛАВСКОЙ ОБЛАСТИ').replace(\
                                                          'ГБУ СО ЯО','ГОСУДАРСТВЕННОЕ БЮДЖЕТНОЕ УЧРЕЖДЕНИЕ СОЦИАЛЬНОГО ОБСЛУЖИВАНИЯ ЯРОСЛАВСКОЙ ОБЛАСТИ')
                            Cust_Word = Cust_Word.replace('КБ ', 'КЛИНИЧЕСКАЯ БОЛЬНИЦА ').replace( \
                                                          '"ИКБ"', '"ИНФЕКЦИОННАЯ КЛИНИЧЕСКАЯ БОЛЬНИЦА"').replace( \
                                                          '"ОКБ"','"ОБЛАСТНАЯ КЛИНИЧЕСКАЯ БОЛЬНИЦА"').replace(\
                                                          'ЦРБ','ЦЕНТРАЛЬНАЯ РАЙОННАЯ БОЛЬНИЦА').replace(\
                                                          'ЯОПБ','ЯРОСЛАВСКАЯ ОБЛАСТНАЯ ПСИХИАТРИЧЕСКАЯ БОЛЬНИЦА').replace( \
                                                          'ССМП','СТАНЦИЯ СКОРОЙ МЕДИЦИНСКОЙ ПОМОЩИ').replace(\
                                                          'ЦМК','ЦЕНТР МЕДИЦИНЫ КАТАСТРОФ').replace(\
                                                          'ЯГИКСПП','ЯРОСЛАВСКИЙ ГОСУДАРСТВЕННЫЙ ИНСТИТУТ КАЧЕСТВА СЫРЬЯ И ПИЩЕВЫХ ПРОДУКТОВ').replace(\
                                                          'ДТИСР АТМР','ДЕПАРТАМЕНТ ТРУДА И СОЦИАЛЬНОГО РАЗВИТИЯ АДМИНИСТРАЦИИ ТУТАЕВСКОГО МУНИЦИПАЛЬНОГО РАЙОНА').replace(\
                                                          'ИМ. ','ИМЕНИ ')


                            INN = ''
                            KPP = ''
                            Org_ID = ''
                            HaveDocs = 0

                            UserAgent = GetRandomUserAgent()

                            headers_cust = {
                               'Accept': '*/*',
                               'Accept-Encoding': 'gzip, deflate, br',
                               'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
                               'Connection': 'keep-alive',
                               'host': 'zakupki.yarregion.ru',
                               'referer': 'https://zakupki.yarregion.ru/reestr-postavshhikov/',
                               'Sec-Fetch-Dest': 'empty',
                               'Sec-Fetch-Mode': 'cors',
                               'Sec-Fetch-Site': 'same-origin',
                               'TE': 'trailers',
                               'user-agent': UserAgent,
                               'x-requested-with': 'XMLHttpRequest'
                                }

                            params = {"uuid": "d65a6e46-2e10-452c-98f9-048fcae44fcc",
                                      "dataVersion": "22.08.2019 14.15.25.520",
                                      "dsCode": "OOS_OOS_007_001_GridData", "paramSearch": Cust_Word, #.upper()
                                      "paramPeriod": "2021-11-30T00:00:00.000Z",
                                      "paramLocal": 0, "codeParam": 0, "_dc": "1638340474585", "page": 1, "start": 0,
                                      "limit": 50}
                            #print(Cust_Word_Orig, Cust_Word)
                            # для организации заказчика
                            r_cust = requests.get(url_cust_api, params=params, headers=headers_cust, verify=False, timeout=50)

                            if r_cust.status_code == 200:
                                try:
                                    r_cust = r_cust.json()
                                    if len(r_cust['data']) > 0:
                                        INN = r_cust['data'][0][1]
                                        KPP = r_cust['data'][0][2]
                                    else:
                                        InsertLog('Получение ИНН, КПП заказчика из реестра не удалось. Ищем через карточку заказчика')
                                        # Если через реестр организаций не нашли, пробуем найти на карточке заказчика (там доступен только ИНН)
                                        # Например: "ГОСУДАРСТВЕННОЕ ПРОФЕССИОНАЛЬНОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ЯРОСЛАВСКОЙ ОБЛАСТИ ЯРОСЛАВСКИЙ ПОЛИТЕХНИЧЕСКИЙ КОЛЛЕДЖ №24 - ИНН: 7605009562"
                                        r_cust2 = requests.get(url_cust_api2, headers=headers, verify=False, timeout=50)
                                        if r_cust2.status_code == 200:
                                            r_cust2 = r_cust2.json()
                                            org_len = len(r_cust2['data'])
                                            i_org = 0
                                            INN = ''
                                            while len(INN) == 0 and i_org < org_len:
                                                if Cust_Word in r_cust2['data'][i_org][2]:
                                                    INN = r_cust2['data'][i_org][2].split('ИНН: ')[1]
                                                i_org += 1
                                        #print(INN, 'KPP=('+KPP+')')
                                except:
                                    # Запись в лог
                                    print('Ошибка при получении ИНН/КПП, url смотреть в логе')
                                    startTime = datetime.datetime.now()
                                    t_log = startTime.strftime('%d.%m.%Y %H:%M:%S')
                                    log = open('./src/Logs/' + str(YLog) + '/Logs_' + str(MLog) + '.txt', 'a+')
                                    log.write(t_log + ' ## Ошибка при получении ИНН/КПП. URL: ' + Url_Notif + '\n')
                                    traceback.print_exc(file=log)
                                    log.close()

                            # Гвозди. Не находит орг. на сайте
                            if len(INN) == 0 and Cust_Word_Orig == 'ДТИСР АТМР':
                                INN = '7611008648'
                                KPP = '761101001'
                            if len(INN) == 0 and Cust_Word_Orig == 'ГБУ ЯО ЯГИКСПП':
                                INN = '7605014107'
                                KPP = '760401001'
                            if len(INN) == 0 and (Cust_Word_Orig == 'МУ ФСК "НЕКРАСОВСКИЙ"' or Cust_Word_Orig == 'МУ ФСК НЕКРАСОВСКИЙ'):
                                INN = '7621011093'
                                KPP = '762101001'
                            if len(INN) == 0 and Cust_Word_Orig == 'ГУЗ ЯО ГОРОДСКАЯ БОЛЬНИЦА №4 Г. РЫБИНСКА':
                                INN = '7610029620'
                                KPP = '761001001'
                            if len(INN) == 0 and Cust_Word_Orig == 'ДЕПАРТАМЕНТ ЗДРАВООХРАНЕНИЯ И ФАРМАЦИИ ЯРОСЛАВСКОЙ ОБЛАСТИ':
                                INN = '7604044726'
                                KPP = '760401001'

                            if len(INN) == 10 and len(KPP) > 0:
                                Org_ID_query = "SELECT top 1 Org_ID from [Cursor].[dbo].[Org] where INN = '" + INN + "' and KPP = '" + KPP + "' order by isnull(isZakupki,0) desc"
                                Org_ID = parser_utils.select_query(Org_ID_query, login_sql, password_sql, 'Cursor', True)
                                #print('hhh',Org_ID)

                            if len(INN) == 9:
                                Org_ID_query = "SELECT top 1 Org_ID from [Cursor].[dbo].[Org] where INN = '" + INN + "' order by isnull(isZakupki,0) desc"
                                Org_ID = parser_utils.select_query(Org_ID_query, login_sql, password_sql, 'Cursor', True)

                            if len(INN) == 10 and len(KPP) == 0:
                                Org_ID_query = "SELECT top 1 Org_ID from [Cursor].[dbo].[Org] where INN = '" + INN + "' order by isnull(isZakupki,0) desc, isnull(isActive,0) desc"
                                Org_ID = parser_utils.select_query(Org_ID_query, login_sql, password_sql, 'Cursor', True)
                                #print('Org_ID None ', Org_ID, len(str(Org_ID)))
                                if len(str(Org_ID)) > 0 and '[]' not in str(Org_ID):
                                    Org_ID_query = "SELECT top 1 Org_ID, KPP from [Cursor].[dbo].[Org] where INN = '" + INN + "' order by isnull(isZakupki,0) desc, isnull(isActive,0) desc"
                                    Org_ID, KPP = parser_utils.select_query(Org_ID_query, login_sql, password_sql, 'Cursor', True)

                            print('Org_ID select: ', Org_ID)


                            if '[]' in str(Org_ID) and len(INN) > 0: #len(str(Org_ID)) == 0 and len(INN) > 0:
                                #print('Нет организации, добавляем по ИНН ' + INN + ', КПП ' + KPP)
                                InsertLog('Нет организации, добавляем по ИНН ' + INN + ', КПП ' + KPP)

                                resp1 = Org_creator.create(INN, login_sql, password_sql)  # добавляем в базу с помощью функции
                                #print(resp1)
                                InsertLog(resp1)

                                Org_ID_query = "SELECT top 1 Org_ID from [Cursor].[dbo].[Org] where INN = '" + INN + "' order by isnull(isZakupki,0) desc, isnull(isActive,0) desc"
                                Org_ID = parser_utils.select_query(Org_ID_query, login_sql, password_sql, 'Cursor', True)
                                if len(str(Org_ID)) > 0 and '[]' not in str(Org_ID):
                                    Org_ID_query = "SELECT top 1 Org_ID, KPP from [Cursor].[dbo].[Org] where INN = '" + INN + "' order by isnull(isZakupki,0) desc, isnull(isActive,0) desc"
                                    Org_ID, KPP = parser_utils.select_query(Org_ID_query, login_sql, password_sql, 'Cursor', True)

                                print('Org_ID (insert NEW) select: ', Org_ID)

                            #sys.exit()

                            # Лог
                            InsertLog('ИНН = ' + INN + ', КПП = ' + KPP + '. Cust_Word_Orig=' + Cust_Word_Orig)

                            #print(INN, KPP)
                            #sys.exit()

                            UserAgent = GetRandomUserAgent()

                            headers = {'Accept': '*/*',
                                       'Accept-Encoding': 'gzip, deflate, br',
                                       'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
                                       'Connection': 'keep-alive',
                                       'content-type': 'application/json',
                                       'host': 'yarregion.ru',
                                       'referer': 'https://zakupki.yarregion.ru/reestr-postavshhikov/',
                                       'Sec-Fetch-Dest': 'empty',
                                       'Sec-Fetch-Mode': 'cors',
                                       'Sec-Fetch-Site': 'same-origin',
                                       'TE': 'trailers',
                                       'user-agent': UserAgent,
                                       'x-requested-with': 'XMLHttpRequest'}

                            headers = {'Accept': '*/*',
                                       'Accept-Encoding': 'gzip, deflate, br',
                                       'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
                                       'Connection': 'keep-alive',
                                       'host': 'zakupki.yarregion.ru',
                                       'referer': 'https://zakupki.yarregion.ru/reestr-postavshhikov/',
                                       'Sec-Fetch-Dest': 'empty',
                                       'Sec-Fetch-Mode': 'cors',
                                       'Sec-Fetch-Site': 'same-origin',
                                       'TE': 'trailers',
                                       'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0',
                                       'x-requested-with': 'XMLHttpRequest'}

                            # Для позиций извещения
                            if needproxy == True:
                                try:
                                    r1_notif = requests.post(url_notif_api, headers=headers, verify=False, proxies=proxies, timeout=50)
                                except requests.exceptions.Timeout as e:
                                    print('Connection timeout (page)')
                                    isError = True
                            else:
                                try:
                                    r1_notif = requests.post(url_notif_api, headers=headers, verify=False, timeout=50)
                                except requests.exceptions.Timeout as e:
                                    InsertLog('Connection timeout (page)')
                                    isError = True

                            # Достаем json для позиций
                            #print(r1_notif)
                            #print(url_notif_api)
                            r00_notif = r1_notif.json()

                            # Для Наименования Извещения
                            if needproxy == True:
                                try:
                                    r1 = requests.post(url_notif_name, headers=headers, verify=False, proxies=proxies, timeout=50)
                                except requests.exceptions.Timeout as e:
                                    print('Connection timeout (page)')
                                    isError = True
                            else:
                                try:
                                    r1_notif_name = requests.post(url_notif_name, headers=headers, verify=False, timeout=50)
                                except requests.exceptions.Timeout as e:
                                    InsertLog('Connection timeout (page1 ПОЗИЦИИ)')
                                    isError = True

                            # Достаем json для Наименования Извещения
                            r00_notif_name = r1_notif_name.json()

                            #print(r00_notif_name)
                            #print(r00_notif_name['data'][0])
                            #sys.exit()

                            if len(r00_notif['data']) == 0:
                                empty = True

                            #print(r0)
                            #sys.exit()
                            r0 = r00_notif['data']

                            #r1 = r0['reg_number']  # Извещение
                            r1 = str(Num_Notif) # Добавить yarregion- т.к. номер может совпасть с номером региона (тоже 7 символов)

                            try:
                                # Полное наименование из второго запроса (со страницы с извещением)
                                r2 = r00_notif_name['data'][0][5]
                            except:
                                # Наименование со страницы поиска (обрезается, если много символов)
                                r2 = r00['data'][num0][2]

                            try:
                                r32 = r00['data'][num0][4] # Заказчик краткое
                            except:
                                r32 = None

                            try:
                                #r4 = r0['planned_contract_date'].replace('-', '')  # План. дата исполнения
                                r4 = r00['data'][num0][6].replace('-', '')
                                try:
                                    r4 = datetime.datetime.strptime(r4, '%Y-%m-%dT%H:%M:%S.%fZ')
                                except:
                                    try:
                                        r4 = datetime.datetime.strptime(r4, '%Y-%m-%dT%H:%M:%S')
                                    except:
                                        try:
                                            r4 = datetime.datetime.strptime(r4, '%Y%m%dT%H:%M:%S.%fZ')
                                        except:
                                            r4 = datetime.datetime.strptime(r4, '%Y%m%dT%H:%M:%S')
                                #r4 = r4.strftime('%Y%m%d %H:%M:%S')
                                r4 = r4.strftime('%Y-%m-%d')
                            except:
                                try:
                                    r4 = r00['data'][num0][6].replace('-', '')
                                    r4 = parse(r4).date().strftime('%Y-%m-%d')
                                except:
                                    r4 = None

                            r5 = len(r0)  # Количество позиций

                            #r6 = r0['start_price']  # Стартовая цена
                            r6 = r00['data'][num0][8].replace('₽', '').replace('руб.', '').replace(r'''\xa''', '').replace(' ', '').replace(' ','').replace(',','.')

                            r7 = ''

                            # try:
                            #     r7 = DeliveryPlace #r0['delivery'][0]['address'] # Адрес (место доставки)
                            # except:
                            #     r7 = None


                            #r8 = r0['status_id'] # Статус
                            r8 = r00['data'][num0][10]

                            # href
                            href = Url_Notif
                            r92 = None  # Подведены итоги



                            try:
                                r93 = Cust_Word  # Наименование заказчика (полное)
                                r10 = INN #r0['organization']['inn']  # ИНН
                                r11 = KPP #r0['organization']['kpp']  # КПП
                            except:
                                r93 = None
                                r10 = None
                                r11 = None

                            r12 = ''
                            # try:
                            #     r12 = DeliveryPlace #r0['organization']['post_address']
                            # except:
                            #     r12 = None

                            r13 = None # Директор ФИО
                            r14 = None # email
                            r15 = None # телефон

                            # try:
                            #     r16 = r0['purchase_method']
                            # except:
                            #     r16 = 'Закупочная сессия'
                            r16 = 'Закупочная сессия'

                            #STEEL от 14.04.2021 нет полей в json
                            #r18 = r0['condition']['ordertype']  # Тип
                            #r19 = r0['condition']['payment']  # Оплата (вид)
                            #r20 = r0['condition']['payment_terms']  # Оплата (тип)

                            # try:
                            #     r18 = r0['order_type']['name']  # Тип: Закупка до 600 000 руб. (п.4 ч.1 ст.93 Закона №44-ФЗ)
                            # except:
                            #     r18 = None

                            r18 = 'Закупка до 600 000 руб. (п.4 ч.1 ст.93 Закона №44-ФЗ)'
                            r19 = None
                            r20 = None


                            r21 = None  # Календарные дни до оконания
                            #r22 = r0['condition']['startprice']  # Другая стартовая цена
                            r22 = None

                            try:
                                #r23 = r0['purchase_start'].replace('-', '')
                                r23 = r00['data'][num0][5].replace('-', '') # Дата размещения
                                try:
                                    r23 = datetime.datetime.strptime(r23, '%Y-%m-%dT%H:%M:%S.%fZ')
                                except:
                                    try:
                                        r23 = datetime.datetime.strptime(r23, '%Y-%m-%dT%H:%M:%S')
                                    except:
                                        try:
                                            r23 = datetime.datetime.strptime(r23, '%Y%m%dT%H:%M:%S.%fZ')
                                        except:
                                            r23 = datetime.datetime.strptime(r23, '%Y%m%dT%H:%M:%S')
                                #r23 = r23.strftime('%Y%m%d %H:%M:%S')
                                r23 = r23.strftime('%Y-%m-%d')
                            except:
                                try:
                                    r23 = r00['data'][num0][5].replace('-', '')
                                    r23 = parse(r23).date().strftime('%Y-%m-%d')
                                except:
                                    r23 = None

                            try:
                                #r24 = r0['purchase_end'].replace('-', '')  # Завершение закупки
                                r24 = r00['data'][num0][6].replace('-', '')
                                r24 = parse(r24).date().strftime('%Y-%m-%d')
                            except:
                                r24 = None


                            try:  # Дата заключения договора
                                #r25 = r0['planned_contract_date']
                                r25 = r00['data'][num0][6].replace('-', '')
                                try:
                                    r25 = datetime.datetime.strptime(r25, '%Y-%m-%dT%H:%M:%S.%fZ')
                                except:
                                    try:
                                        r25 = datetime.datetime.strptime(r25, '%Y-%m-%dT%H:%M:%S')
                                    except:
                                        try:
                                            r25 = datetime.datetime.strptime(r25, '%Y%m%dT%H:%M:%S.%fZ')
                                        except:
                                            r25 = datetime.datetime.strptime(r25, '%Y%m%dT%H:%M:%S')
                                #r25 = r25.strftime('%Y%m%d %H:%M:%S')
                                r25 = r25.strftime('%Y-%m-%d')
                            except:
                                try:
                                    r25 = r00['data'][num0][6].replace('-', '')
                                    r25 = parse(r25).date().strftime('%Y-%m-%d')
                                except:
                                    r25 = None

                            r26 = None  # регион поставки

                            # try:
                            #     r27 = r0['delivery'][0]['address'] # Адрес самовывоза
                            # except:
                            #     r27 = None
                            r27 = None

                            r28 = None

                            r29 = r8


                            # Документация
                            try:
                                if needproxy == True:
                                    try:
                                        r1 = requests.post(url_docs, headers=headers, verify=False, proxies=proxies,
                                                           timeout=50)
                                    except requests.exceptions.Timeout as e:
                                        print('Connection timeout (page2_docs)')
                                else:
                                    try:
                                        r1_docs = requests.post(url_docs, headers=headers, verify=False, timeout=50)
                                    except requests.exceptions.Timeout as e:
                                        InsertLog('Connection timeout (page2 docs)')

                                r00_docs = r1_docs.json()

                                if len(r00_docs['data']) > 0:
                                        HaveDocs = 1
                            except:
                                pass

                            # Позиции
                            i_pos = 0
                            len_pos = len(r0)

                            x1 = None
                            x2 = None
                            x3 = None
                            x4 = None
                            x5 = 0
                            x5_summ = 0

                            while i_pos < len_pos:
                                try:
                                    x1 = r0[i_pos][1]  # Наименование позиции
                                    x2 = r0[i_pos][4].split(' - ')[0].replace(',00', '').replace(',', '.')  # Количество, например: 15000,00 - см[3*];^мл
                                    #x3 = 'упак.'
                                    x3 = r0[i_pos][4].split(' - ')[1]  # Ед измер. 'упак.', 'см[3*];^мл'

                                    x4 = r0[i_pos][3].split(' - ')[0]  # ОКПД, например: 21.20.10.191 - Препараты антибактериальные для системного использования

                                    x5 = 0  # Цена
                                    x5_summ = 0  # Сумма

                                except:
                                    pass
                                    # x1 = None
                                    # x2 = None
                                    # x3 = None
                                    # x4 = None
                                    # x5 = None
                                    # x5_summ = None

                                # Проверка на число
                                if is_number(x2) == False and is_int(x2) == False:
                                    x2 = 1

                                x7 = x1

                                pos_name.append(x1)
                                pos_qty.append(x2)
                                pos_value.append(x3)
                                pos_notifnr.append(r1)
                                izv_name.append(r2)
                                pos_okpd.append(x4)
                                pos_price.append(x5)
                                pos_summ.append(x5_summ)
                                pos_form.append(x7)

                                i_pos += 1

                            # if num0 == 9:
                            # print(pos_name)
                            # print(pos_qty)

                            e1.append(r1)
                            e2.append(r2)
                            e3.append(r32)
                            e4.append(r4)
                            e5.append(r5)
                            e6.append(r6)
                            e7.append(r7)
                            e8.append(r8)
                            e9.append(href)
                            e10.append(r92)
                            e11.append(r93)
                            e12.append(r10)
                            e13.append(r11)
                            e14.append(r12)
                            e15.append(r13)
                            e16.append(r14)
                            e17.append(r15)
                            e18.append(r16)
                            e19.append(r2)
                            e20.append(r18)
                            e21.append(r19)
                            e22.append(r20)
                            e23.append(r21)
                            e24.append(r22)
                            e25.append(r23)
                            e26.append(r24)
                            e27.append(r25)
                            e28.append(r26)
                            e29.append(r27)
                            e30.append(r28)
                            status.append(r29)
                            HaveDocs_mass.append(HaveDocs)

                            num0 += 1
                            bdcount -= 1
                        else:
                            e1 = pd.DataFrame(e1)
                            e2 = pd.DataFrame(e2)
                            e3 = pd.DataFrame(e3)
                            e4 = pd.DataFrame(e4)
                            e5 = pd.DataFrame(e5)
                            e6 = pd.DataFrame(e6)
                            e7 = pd.DataFrame(e7)
                            e8 = pd.DataFrame(e8)
                            e9 = pd.DataFrame(e9)
                            e10 = pd.DataFrame(e10)
                            e11 = pd.DataFrame(e11)
                            e12 = pd.DataFrame(e12)
                            e13 = pd.DataFrame(e13)
                            e14 = pd.DataFrame(e14)
                            e15 = pd.DataFrame(e15)
                            e16 = pd.DataFrame(e16)
                            e17 = pd.DataFrame(e17)
                            e18 = pd.DataFrame(e18)
                            e19 = pd.DataFrame(e19)
                            e20 = pd.DataFrame(e20)
                            e21 = pd.DataFrame(e21)
                            e22 = pd.DataFrame(e22)
                            e23 = pd.DataFrame(e23)
                            e24 = pd.DataFrame(e24)
                            e25 = pd.DataFrame(e25)
                            e26 = pd.DataFrame(e26)
                            e27 = pd.DataFrame(e27)
                            e28 = pd.DataFrame(e28)
                            e29 = pd.DataFrame(e29)
                            e30 = pd.DataFrame(e30)
                            status = pd.DataFrame(status)
                            HaveDocs_mass = pd.DataFrame(HaveDocs_mass)

                            pos_name = pd.DataFrame(pos_name)
                            pos_qty = pd.DataFrame(pos_qty)
                            pos_value = pd.DataFrame(pos_value)
                            pos_notifnr = pd.DataFrame(pos_notifnr)
                            izv_name = pd.DataFrame(izv_name)
                            pos_okpd = pd.DataFrame(pos_okpd)
                            pos_price = pd.DataFrame(pos_price)
                            pos_summ = pd.DataFrame(pos_summ)
                            pos_form = pd.DataFrame(pos_form)
                            pass


                        bdcount = 10
                        #  Собираем всё воедино
                        # Для извещений

                        df_not = pd.concat(
                            [e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20,
                             e21, e22, e23, e24, e25, e26, e27, e28, e29, e30, status, HaveDocs_mass], axis=1)

                        if df_not.empty == True:
                            empty = True
                        else:
                            df_not.columns = columns_not
                            # Исключения из названий торгов (накопились в процессе работы)
                            df_not = df_not[~df_not['Наименование'].str.contains(r'(?i)строител|(?i)инженерное|'
                                                                                 r'(?i)оборудование|(?i)ветеринар|(?i)мебель|'
                                                                                 r'(?i)автомобил|(?i)асфальт|(?i)венок|(?i)обучение|(?i)канцеляр|'
                                                                                 r'(?i)красот|(?i)телефон|'
                                                                                 r'(?i)раков|(?i)считыват|(?i)химич|'
                                                                                 r'(?i)огород|(?i)служебн|(?i)1с|(?i)индвидуал|'
                                                                                 r'(?i)комплект|(?i)метролог|(?i)моющ|'
                                                                                 r'(?i)ремонт|(?i)насос|(?i)светил|'
                                                                                 r'(?i)диван|(?i)унитаз|(?i)сантех|'
                                                                                 r'(?i)сооруж|(?i)хозтовар|(?i)связи|'
                                                                                 r'(?i)фильтр|(?i)квалификации|(?i)материал|'
                                                                                 r'(?i)отход|(?i)хозяйств|(?i)совермен|'
                                                                                 r'(?i)батар|(?i)запах|(?i)железнодор|'
                                                                                 r'(?i)технический|(?i)пропан|(?i)перевозк|'
                                                                                 r'(?i)собак|(?i)отбеливат|(?i)связь|'
                                                                                 r'(?i)подушк|(?i)населен|(?i)корреспонд|'
                                                                                 r'(?i)гражд|(?i)почт|(?i)шкатулк|'
                                                                                 r'(?i)пластин|(?i)журнал|(?i)запчаст|'
                                                                                 r'(?i)заправ|(?i)учеб|(?i)уничтож|'
                                                                                 r'(?i)метал|(?i)осмотр|(?i)услуг|(?i)Услуг|'
                                                                                 r'(?i)программ|(?i)животн|(?i)пожар|'
                                                                                 r'(?i)аи-9|(?i)пшено|(?i)телята|(?i)молочная|'
                                                                                 r'(?i)ультрабук|(?i)сварочн|(?i)кондитерские|(?i)овощи|(?i)бакалея|(?i)сосиски|'
                                                                                 
                                                                                 #STEEL от 15.04.2021
                                                                                 r'(?i)методич|(?i)печатн|(?i)сухофрукты|'
                                                                                 r'(?i)документации|(?i)правочник|(?i)переподготовк|(?i)типограф|'
                                                                                 r'(?i)одписка|(?i)открытки|(?i)кспертизы|(?i)кабель|(?i)оказание|'
                                                                                 r'(?i)помощь|(?i)исследования|(?i)одежда|(?i)обувь|(?i)актуальные вопросы',
                                                                                 na=False, regex=True)]

                        try:
                            x = df_not['Номер извещения'][0]
                        except:
                            empty = True

                        df_pos = pd.concat([pos_name, pos_qty, pos_value, pos_notifnr, izv_name, pos_okpd, pos_price, pos_summ, pos_form], axis=1)

                        if df_not.empty != True and df_pos.empty != True:  # STEEL от 22.11.2021 добавил and df_pos.empty != True
                            # проставляем статус
                            df_not['Status'] = df_not['Status'].replace('Завершен', '4').replace('Отменен', '5').replace('Опубликован', '7')

                            #print(df_not)
                            #sys.exit()

                            # Для позиций
                            #df_pos = pd.concat([pos_name, pos_qty, pos_value, pos_notifnr, izv_name, pos_okpd, pos_price, pos_summ, pos_form], axis=1)
                            df_pos.columns = columns_pos

                            # убираем позиции извещений, которые были отфильтрованы
                            #df_pos = df_pos[df_pos['Извещение'].isin(df_not['Номер извещения'])].dropna()
                            df_pos = df_pos[df_pos['Извещение'].isin(df_not['Номер извещения'])] #убрал .dropna()
                            #print(df_pos)

                            # Запись в лог
                            InsertLog('Исправляем позиции')

                            #STEEL от 22.04.2021 убрать строки с пустым наименованием
                            df_pos.dropna(subset=['Наименование товара (работ, услуг)'], inplace=True)

                            df_pos['лекар'] = df_pos['Наименование товара (работ, услуг)']
                            #df_pos['форма'] = df_pos['Наименование товара (работ, услуг)']  #STEEL  добавил форму

                            df_pos['unit'] = 'лекар'
                            df_pos['isDrug'] = '0'


                            p1 = 0
                            while p1 < len(df_pos):

                                try:
                                    df_pos['Единица измерения'].iloc[p1] = df_pos['Единица измерения'].iloc[p1].replace(
                                        'усл. ед', 'упак.')
                                except:
                                    pass

                                if 'ЗРСК' in df_pos['лекар'].iloc[p1]:
                                    df_pos['лекар'].iloc[p1] = df_pos['лекар'].iloc[p1].replace('ЗРСК ', '')
                                if 'ЕГК' in df_pos['лекар'].iloc[p1]:
                                    df_pos['лекар'].iloc[p1] = df_pos['лекар'].iloc[p1].replace('ЕГК ', '')
                                if 'ТЛД' in df_pos['лекар'].iloc[p1]:
                                    df_pos['лекар'].iloc[p1] = df_pos['лекар'].iloc[p1].replace('ТЛД ', '')
                                if 'СТП' in df_pos['лекар'].iloc[p1]:
                                    df_pos['лекар'].iloc[p1] = df_pos['лекар'].iloc[p1].replace('СТП ', '')
                                if 'ПШК' in df_pos['лекар'].iloc[p1]:
                                    df_pos['лекар'].iloc[p1] = df_pos['лекар'].iloc[p1].replace('ПШК ', '')
                                if 'МНН - ' in df_pos['лекар'].iloc[p1]:
                                    df_pos['лекар'].iloc[p1] = df_pos['лекар'].iloc[p1].replace('МНН - ', '')
                                if 'МНН ' in df_pos['лекар'].iloc[p1]:
                                    df_pos['лекар'].iloc[p1] = df_pos['лекар'].iloc[p1].replace('МНН ', '')

                                if ' (' in df_pos['лекар'].iloc[p1] and ('ЖЕЛЕЗА' not in df_pos['лекар'].iloc[p1]
                                                                         and 'железа' not in df_pos['лекар'].iloc[p1]
                                                                         and 'Железа' not in df_pos['лекар'].iloc[p1]):
                                    df_pos['лекар'].iloc[p1] = df_pos['лекар'].iloc[p1].split(' (')[0]

                                try:
                                    df_pos['лекар'].iloc[p1] = df_pos['лекар'].iloc[p1].rstrip()
                                except:
                                    pass
                                try:
                                    df_pos['лекар'].iloc[p1] = df_pos['лекар'].iloc[p1].lstrip()
                                except:
                                    pass

                                if len(df_pos['лекар'].iloc[p1]) > 0:
                                    if df_pos['лекар'].iloc[p1][-1] == '-':
                                        df_pos['лекар'].iloc[p1] = df_pos['лекар'].iloc[p1].replace(df_pos['лекар'].iloc[p1][-1], '')

                                if len(df_pos['лекар'].iloc[p1]) == 0:
                                    df_pos['лекар'].iloc[p1] = 'ЛЕКАРСТВЕННЫЕ СРЕДСТВА'

                                if '0' in df_pos['isDrug'].iloc[p1]:
                                    m_izv_name.append(r2)
                                    m_pos_name.append(df_pos['лекар'].iloc[p1])
                                    m_pos_form.append(df_pos['форма'].iloc[p1])
                                    m_link.append(href)
                                    m_notifnr.append(r1)

                                p1 += 1

                            #if isDebug == True:
                                #df_not.to_excel('notifications_parsed.xlsx')
                                #df_pos.to_excel('positions_parsed.xlsx')
                            # sys.exit()

                            # Запись в лог
                            InsertLog('Заносим данные в базу')

                            from datetime import datetime as dt
                            t1 = dt.now().strftime('%Y-%m-%d')

                            n = 0
                            leng = len(df_not)
                            while n < leng:
                                x1 = df_not.iloc[n]

                                #if num0 == 9:
                                    #print(x1)
                                    #sys.exit()

                                duplicate_query = "select n.notificationNumber " \
                                                  "FROM [CursorImport].[import].[Notifications44] n (nolock) "\
                                                   "inner join [CursorImport].[import].CustomerRequirements44 c (nolock) on n.id_Notification=c.id_Notification "\
                                                   "inner join [CursorImport].[import].Lots44 l (nolock) on n.id_Notification=l.id_Notification "\
                                                   "where n.notificationNumber = '" + str(x1[0]) + "'"
                                duplicate_list = parser_utils.select_query(duplicate_query, login_sql, password_sql, isList=True)

                                #STEEL от 21.04.2021 Если нет позиций у изв., то такое не закачиваем
                                pos_temp1 = df_pos[df_pos['Извещение'].isin([x1[0]])]  # Берем позиции с этим номером извещений из таблицы
                                len_position = len(pos_temp1)

                                #print(df_pos)
                                #print(duplicate_list)

                                if duplicate_list:
                                    if isArgs == False:
                                        #print('Извещение есть в базе, не закачиваем: ' + str(x1[0]))
                                        # Запись в лог
                                        InsertLog('Извещение есть в базе, не закачиваем: ' + str(x1[0]))

                                        # Если 10 повторов, то прекращаем цикл (описание перед циклом)
                                        ncount = ncount + 1
                                        if ncount > 10:
                                            print('Переход к следующему слову для поиска.')

                                    else:  # если были даны арги,то удаляем из импорта
                                        conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                                              'Server=37.203.243.65\CURSORMAIN,49174;'
                                                              'Database=CursorImport;'
                                                              'UID='+login_sql+';'
                                                              'PWD='+password_sql+';'
                                                              'Trusted_Connection=no;')
                                        cursor = conn.cursor()
                                        cursor.execute("delete p " \
                                                  "FROM [CursorImport].[import].[Notifications44] n (nolock) "\
                                                   "inner join [CursorImport].[import].CustomerRequirements44 c (nolock) on n.id_Notification=c.id_Notification "\
                                                   "inner join [CursorImport].[import].Lots44 l (nolock) on n.id_Notification=l.id_Notification "\
                                                   "inner join [CursorImport].[import].Products44 p (nolock) on n.id_Notification=p.id_Notification "\
                                                   "where n.notificationNumber = '" + str(x1[0]) + "'")
                                        cursor.execute("delete l " \
                                                  "FROM [CursorImport].[import].[Notifications44] n (nolock) "\
                                                   "inner join [CursorImport].[import].CustomerRequirements44 c (nolock) on n.id_Notification=c.id_Notification "\
                                                   "inner join [CursorImport].[import].Lots44 l (nolock) on n.id_Notification=l.id_Notification "\
                                                   "where n.notificationNumber = '" + str(x1[0]) + "'")
                                        cursor.execute("delete c " \
                                                  "FROM [CursorImport].[import].[Notifications44] n (nolock) "\
                                                   "inner join [CursorImport].[import].CustomerRequirements44 c (nolock) on n.id_Notification=c.id_Notification "\
                                                   "where n.notificationNumber = '" + str(x1[0]) + "'")
                                        cursor.execute("delete n " \
                                                  "FROM [CursorImport].[import].[Notifications44] n (nolock) "\
                                                   "where n.notificationNumber = '" + str(x1[0]) + "'")
                                        if isDebug == False:
                                            conn.commit()
                                        conn.close()

                                        print('Loading ' + str(x1[0]))  # Берем номер извещения
                                        print('Status: ' + str(x1[30]))
                                        print('HaveDocs: ' + str(x1[31]))

                                        # Запись в лог
                                        InsertLog('Loading ' + str(x1[0]) + '. Status: ' + str(x1[30]), 1)


                                        conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                                              'Server=37.203.243.65\CURSORMAIN,49174;'
                                                              'Database=CursorImport;'
                                                              'UID=' + login_sql + ';'
                                                                                   'PWD=' + password_sql + ';'
                                                                                                           'Trusted_Connection=no;')
                                        cursor = conn.cursor()

                                        # Notifications44

                                        # sys.exit()

                                        cursor.execute(
                                            "insert into [CursorImport].[import].[Notifications44](id_Notification, notificationNumber, createDate, publishDate, href, orderName, PlacingWay_Code, PlacingWay_Name, placer_INN, placer_fullName, notificationCommission_p1Place, notificationCommission_p2Place, notificationCommission_p3Date, EP_name, EP_url, LotCount, ImportType, DTCreate, placer_KPP, placer_Addr, createDateT, publishDateT, notificationCommission_p1DateT, notificationCommission_p2DateT, notificationCommission_p3DateT) "
                                            "values (?, ?, ?, ?, ?, ?, 'EF', ?, ?, ?, 'yarregion.ru', 'yarregion.ru', CAST(GETDATE() as DATE), 'yarregion.ru', 'https://zakaz.yarregion.ru', '1', '17', GETDATE(), ?, ?, GETDATE(), ?, ?, ?, ?) ",
                                            str(x1[0]), str(x1[0]), str(t1), str(t1), str(x1[8]), str(x1[1]),
                                            str(x1[19]), #PlacingWay_Name Закупка до 600 000 руб. (п.4 ч.1 ст.93 Закона №44-ФЗ)
                                            str(x1[11]), str(x1[10]), str(x1[12]), str(x1[13])
                                            , str(x1[24]), str(x1[24]) #str(x1[25]), str(x1[25])
                                            , str(x1[26]), str(x1[26]))

                                        # , str(x1[24]), str(x1[24])
                                        # , str(x1[25]), str(x1[25]))

                                        # yarregion_status
                                        cursor.execute(
                                            "insert into [CursorImport].[log].[yarregion_status](id_Notification, NotifNr, Status, DTCreate, HaveDocs) "
                                            "values (?, ?, ?, GETDATE(), ?) ",
                                            str(x1[0]), str(x1[0]), str(x1[30]), x1[31])

                                        # Lots44
                                        cursor.execute(
                                            "declare @i int; Select @i = coalesce(MAX(id),0) + 1 FROM [CursorImport].[import].[Lots44]; "
                                            "declare @i_lot int; Select @i_lot = coalesce(MAX(id_Lot),0) + 1 FROM [CursorImport].[import].[Lots44]; "
                                            "SET IDENTITY_INSERT [CursorImport].[import].[Lots44] ON "
                                            "insert into [CursorImport].[import].[Lots44](id, id_Lot, id_Notification, ordinalNumber, subject, ImportType, DTCreate, maxPrice) "
                                            "values (@i, @i_lot, ?, '1', ?, '17', GETDATE(), ?) "
                                            "SET IDENTITY_INSERT [CursorImport].[import].[Lots44] OFF ",
                                            str(x1[0]),
                                            str(x1[1]),
                                            # str(x1[2]),
                                            str(x1[5]))

                                        # CustomerRequirements44
                                        cursor.execute(
                                            "declare @i_req int; Select @i_req = coalesce(MAX(id_CustomerRequirement),0) + 1 FROM [CursorImport].[import].[CustomerRequirements44]; "
                                            "declare @lot_id int; Select @lot_id = id_lot FROM [CursorImport].[import].[Lots44] where id_Notification = '" + str(
                                                str(x1[0])) + "'; "
                                                              "insert into [CursorImport].[import].[CustomerRequirements44](id_CustomerRequirement, id_Notification, id_Lot, maxPrice, customer_INN, customer_fullName, deliveryPlace, ImportType, DTCreate, customer_KPP, customer_Addr, financeSource) "
                                                              "values (@i_req, ?, @lot_id, ?, ?, ?, ?, '17', GETDATE(), ?, ?, 'Средства учреждений') ",
                                            str(x1[0]), str(x1[5]), str(x1[11]), str(x1[10]), str(x1[13]), str(x1[12]),
                                            str(x1[13]))
                                        if isDebug == False:
                                            conn.commit()
                                        conn.close()

                                        # Загрузка позиций в базу
                                        pos_temp = df_pos[df_pos['Извещение'].isin(
                                            [x1[0]])]  # Берем позиции с этим номером извещений из таблицы
                                        len8 = len(pos_temp)

                                        n1 = 0
                                        while n1 < len8:
                                            p1 = pos_temp['Наименование товара (работ, услуг)'].iloc[n1]
                                            p2 = pos_temp['Количество'].iloc[n1]
                                            p3 = pos_temp['Единица измерения'].iloc[n1]

                                            p4 = pos_temp['лекар'].iloc[n1]
                                            p5 = pos_temp['форма'].iloc[n1]
                                            p_unit = pos_temp['unit'].iloc[n1]
                                            p_drug = pos_temp['isDrug'].iloc[n1]

                                            # STEEL от 16.04.2021 добавил окпд для загрузки на импорт
                                            p_okpd = pos_temp['окпд'].iloc[n1]

                                            p_price = pos_temp['цена'].iloc[n1]
                                            p_summ = pos_temp['сумма'].iloc[n1]
                                            #print(p_price)

                                            try:
                                                float(p_price)
                                            except:
                                                p_price = 0

                                            try:
                                                float(p_summ)
                                            except:
                                                p_summ = 0

                                            if p_summ == 0 and p_price != 0:
                                                p_summ = p2 * p_price

                                            #if p_price.isdigit() == False or p_price == None:
                                            #    p_price = 0

                                            # Проставить isDrug у лекарств, далее dbo.GetProdType_bea проставит тип продукции L, т.к. isDrug будет =1
                                            p_drug = 0

                                            if p_okpd != None and p_okpd != '' and p_okpd[0] == '2' and p_okpd[1] == '1':
                                                p_drug = 1

                                            if p_drug == 0 and (p_okpd == None or p_okpd == ''):
                                                p_drug = 1

                                            if p_okpd == 'None' or p_okpd == '-':
                                                p_okpd = None

                                            conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                                                  'Server=37.203.243.65\CURSORMAIN,49174;'
                                                                  'Database=CursorImport;'
                                                                  'UID=' + login_sql + ';'
                                                                                       'PWD=' + password_sql + ';'
                                                                                                               'Trusted_Connection=no;')
                                            cursor = conn.cursor()

                                            # Products44
                                            if len8 == 1:
                                                cursor.execute(
                                                    "declare @lot_id int; Select @lot_id = id_lot FROM [CursorImport].[import].[Lots44] where id_Notification = '" + str(
                                                        str(x1[0])) + "'; "
                                                                      "insert into [CursorImport].[import].[Products44](id_Notification, id_Lot, code, name, ImportType, DTCreate, ProdName, unit, price, quantity, sum, isDrug, Form) "
                                                                      "values (?, @lot_id, ?, ?, '17', GETDATE(), ?, ?, ?, ?, ?, ?, ?) ",
                                                    # "values (?, @lot_id, ?, ?, '17', GETDATE(), ?, ?, ?, ?, ?, ?, ?) ",
                                                    str(x1[0]), str(p_okpd), str(p_unit), str(p4), str(p3),
                                                    str(p_price), #str(x1[5] / float(p2)),
                                                    str(p2),
                                                    str(x1[5]), str(p_drug), str(p5))
                                            else:
                                                cursor.execute(
                                                    "declare @lot_id int; Select @lot_id = id_lot FROM [CursorImport].[import].[Lots44] where id_Notification = '" + str(
                                                        str(x1[0])) + "'; "
                                                                      "insert into [CursorImport].[import].[Products44](id_Notification, id_Lot, code, name, ImportType, DTCreate, ProdName, unit, price, quantity, sum, isDrug, Form) "
                                                                      "values (?, @lot_id, ?, ?, '17', GETDATE(), ?, ?, ?, ?, ?, ?, ?) ",
                                                    # "values (?, @lot_id, ?, ?, '17', GETDATE(), ?, ?, ?, ?, ?, ?, ?) ",
                                                    str(x1[0]), str(p_okpd), str(p_unit), str(p4), str(p3),
                                                    str(p_price),  # None,
                                                    str(p2), str(p_summ), #str(p2 * p_price),  # None,
                                                    str(p_drug),
                                                    str(p5))
                                            if isDebug == False:
                                                conn.commit()
                                            conn.close()

                                            n1 += 1

                                elif len_position > 0:
                                    print('Loading ' + str(x1[0]))  # Берем номер извещения
                                    print('Status: ' + str(x1[30]))
                                    print('HaveDocs: ' + str(x1[31]))

                                    # Запись в лог
                                    InsertLog('Loading ' + str(x1[0]) + '. Status: ' + str(x1[30]), 1)

                                    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                                          'Server=37.203.243.65\CURSORMAIN,49174;'
                                                          'Database=CursorImport;'
                                                          'UID=' + login_sql + ';'
                                                          'PWD=' + password_sql + ';'
                                                           'Trusted_Connection=no;')
                                    cursor = conn.cursor()

                                    # Notifications44
                                    # print(str(x1[0]),
                                    #     str(x1[1]),
                                    #     #str(x1[2]),
                                    #     str(x1[5]))
                                    # print(str(x1[30]), str(x1[5]), str(x1[0]), str(x1[5]), int(x1[11]), str(x1[10]), str(x1[13]), int(x1[12]),
                                    #     str(x1[13]))
                                    #sys.exit()

                                    cursor.execute(
                                        "insert into [CursorImport].[import].[Notifications44](id_Notification, notificationNumber, createDate, publishDate, href, orderName, PlacingWay_Code, PlacingWay_Name, placer_INN, placer_fullName, notificationCommission_p1Place, notificationCommission_p2Place, notificationCommission_p3Date, EP_name, EP_url, LotCount, ImportType, DTCreate, placer_KPP, placer_Addr, createDateT, publishDateT, notificationCommission_p1DateT, notificationCommission_p2DateT, notificationCommission_p3DateT) "
                                        "values (?, ?, ?, ?, ?, ?, 'EF', ?, ?, ?, 'yarregion.ru', 'yarregion.ru', CAST(GETDATE() as DATE), 'yarregion.ru', 'https://zakaz.yarregion.ru/', '1', '17', GETDATE(), ?, ?, GETDATE(), ?, ?, ?, ?) ",
                                        str(x1[0]), str(x1[0]), str(t1), str(t1),str(x1[8]), str(x1[1]),
                                        str(x1[19]),
                                        str(x1[11]), str(x1[10]), str(x1[12]),
                                        str(x1[13])
                                        , str(x1[24]), str(x1[24]) #str(x1[25]), str(x1[25])
                                        , str(x1[26]), str(x1[26]))

                                        #, str(x1[24]), str(x1[24])
                                        #, str(x1[25]), str(x1[25]))

                                    # yarregion_status
                                    cursor.execute(
                                        "insert into [CursorImport].[log].[yarregion_status](id_Notification, NotifNr, Status, DTCreate, HaveDocs) "
                                        "values (?, ?, ?, GETDATE(), ?) ",
                                        str(x1[0]), str(x1[0]), str(x1[30]), int(x1[31]))

                                    # Lots44
                                    cursor.execute(
                                        "declare @i int; Select @i = coalesce(MAX(id),0) + 1 FROM [CursorImport].[import].[Lots44]; "
                                        "declare @i_lot int; Select @i_lot = coalesce(MAX(id_Lot),0) + 1 FROM [CursorImport].[import].[Lots44]; "
                                        "SET IDENTITY_INSERT [CursorImport].[import].[Lots44] ON "
                                        "insert into [CursorImport].[import].[Lots44](id, id_Lot, id_Notification, ordinalNumber, subject, ImportType, DTCreate, maxPrice) "
                                        "values (@i, @i_lot, ?, '1', ?, '17', GETDATE(), ?) "
                                        "SET IDENTITY_INSERT [CursorImport].[import].[Lots44] OFF ",
                                        str(x1[0]),
                                        str(x1[1]),
                                        #str(x1[2]),
                                        str(x1[5]))

                                    # CustomerRequirements44
                                    cursor.execute(
                                        "declare @i_req int; Select @i_req = coalesce(MAX(id_CustomerRequirement),0) + 1 FROM [CursorImport].[import].[CustomerRequirements44]; "
                                        "declare @lot_id int; Select @lot_id = id_lot FROM [CursorImport].[import].[Lots44] where id_Notification = '" + str(
                                            str(x1[0])) + "'; "
                                                          "insert into [CursorImport].[import].[CustomerRequirements44](id_CustomerRequirement, id_Notification, id_Lot, maxPrice, customer_INN, customer_fullName, deliveryPlace, ImportType, DTCreate, customer_KPP, customer_Addr, financeSource) "
                                                          "values (@i_req, ?, @lot_id, ?, ?, ?, ?, '17', GETDATE(), ?, ?, 'Средства учреждений') ",
                                        str(x1[0]), str(x1[5]), str(x1[11]), str(x1[10]), str(x1[13]), str(x1[12]),
                                        str(x1[13]))
                                    if isDebug == False:
                                        conn.commit()
                                    conn.close()

                                    # Загрузка позиций в базу
                                    pos_temp = df_pos[df_pos['Извещение'].isin([x1[0]])]  # Берем позиции с этим номером извещений из таблицы
                                    len8 = len(pos_temp)

                                    n1 = 0
                                    while n1 < len8:
                                        p1 = pos_temp['Наименование товара (работ, услуг)'].iloc[n1]
                                        p2 = pos_temp['Количество'].iloc[n1]
                                        p3 = pos_temp['Единица измерения'].iloc[n1]

                                        p4 = pos_temp['лекар'].iloc[n1]
                                        p5 = pos_temp['форма'].iloc[n1]
                                        p_unit = pos_temp['unit'].iloc[n1]
                                        p_drug = pos_temp['isDrug'].iloc[n1]

                                        #STEEL от 16.04.2021 добавил окпд для загрузки на импорт
                                        p_okpd = pos_temp['окпд'].iloc[n1]

                                        p_price = pos_temp['цена'].iloc[n1]
                                        p_summ = pos_temp['сумма'].iloc[n1]

                                        try:
                                            float(p_price)
                                        except:
                                            p_price = 0

                                        try:
                                            float(p_summ)
                                        except:
                                            p_summ = 0

                                        if p_summ == 0 and p_price != 0:
                                            p_summ = p2 * p_price

                                        try:
                                            if p_price == None or p_price == 'nan': #nan:
                                                p_price = 0
                                        except:
                                            pass


                                        if len8 == 1 and p_price == 0 and p2 != 0:
                                            p_price = float(x1[5]) / float(p2)

                                        #if p_price.isdigit() == False or p_price == None:
                                        #    p_price = 0

                                        #Проставить isDrug у лекарств, далее dbo.GetProdType_bea проставит тип продукции L, т.к. isDrug будет =1
                                        p_drug = 0
                                        if p_okpd != None and p_okpd != '' and p_okpd[0] == '2' and p_okpd[1] == '1':
                                            p_drug = 1

                                        if p_drug == 0 and (p_okpd == None or p_okpd == '' or p_okpd == 'None' or p_okpd == '-'):
                                            p_drug = 1
                                            p_okpd = '21'

                                        #if p_okpd == 'None' or p_okpd == '-':
                                        #    p_okpd = None



                                        conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                                              'Server=37.203.243.65\CURSORMAIN,49174;'
                                                              'Database=CursorImport;'
                                                              'UID=' + login_sql + ';'
                                                               'PWD=' + password_sql + ';'
                                                               'Trusted_Connection=no;')
                                        cursor = conn.cursor()

                                        # print(str(x1[0]), str(p_okpd), str(p_unit), str(p4), str(p3), str(p_price), #None,
                                        #         str(p2),  #None,
                                        #         str(p_drug),
                                        #         str(p5))
                                        # print(str(p_summ))
                                        # print(str(float(p2) * float(p_price)))
                                        #sys.exit()

                                        # Products44
                                        if len8 == 1:
                                            cursor.execute(
                                                "declare @lot_id int; Select @lot_id = id_lot FROM [CursorImport].[import].[Lots44] where id_Notification = '" + str(
                                                    str(x1[0])) + "'; "
                                                                  "insert into [CursorImport].[import].[Products44](id_Notification, id_Lot, code, name, ImportType, DTCreate, ProdName, unit, price, quantity, sum, isDrug, Form) "
                                                                  "values (?, @lot_id, ?, ?, '17', GETDATE(), ?, ?, ?, ?, ?, ?, ?) ",
                                                                  #"values (?, @lot_id, ?, ?, '17', GETDATE(), ?, ?, ?, ?, ?, ?, ?) ",
                                                str(x1[0]), str(p_okpd), str(p_unit), str(p4), str(p3),
                                                str(p_price), #str(x1[5] / float(p2)),
                                                str(p2),
                                                str(x1[5]),
                                                str(p_drug), str(p5))
                                        else:
                                            cursor.execute(
                                                "declare @lot_id int; Select @lot_id = id_lot FROM [CursorImport].[import].[Lots44] where id_Notification = '" + str(
                                                    str(x1[0])) + "'; "
                                                                  "insert into [CursorImport].[import].[Products44](id_Notification, id_Lot, code, name, ImportType, DTCreate, ProdName, unit, price, quantity, sum, isDrug, Form) "
                                                                  "values (?, @lot_id, ?, ?, '17', GETDATE(), ?, ?, ?, ?, ?, ?, ?) ",
                                                                  #"values (?, @lot_id, ?, ?, '17', GETDATE(), ?, ?, ?, ?, ?, ?, ?) ",
                                                str(x1[0]), str(p_okpd), str(p_unit), str(p4), str(p3), str(p_price), #None,
                                                str(p2), str(p_summ), #str(p2 * p_price), #None,
                                                str(p_drug),
                                                str(p5))
                                        if isDebug == False:
                                            conn.commit()
                                        conn.close()

                                        n1 += 1
                                else:
                                    #print('Нет позиций у извещения: ' + str(x1[0]))

                                    # Запись в лог
                                    InsertLog('Нет позиций у извещения: ' + str(x1[0]))

                                print('\n')
                                n += 1

                        else:
                            # Запись в лог
                            InsertLog('Нет извещений для закачки')


                    else:
                        print(colored('Парсинг извещений завершен', 'blue'))

                        # Запись в лог
                        InsertLog('Парсинг извещений завершен', 1)

                        ncount = 0
                        collected = True
                        time.sleep(2)
                        isNotDone = False
                except KeyboardInterrupt:
                    isNotDone = False
                except:
                    if empty == True and collected == True:
                        #print('Нет данных по этому слову')

                        # Запись в лог
                        InsertLog('Нет данных по этому слову')

                        isNotDone = False
                    elif empty == False and collected == True:
                        #print('Все данные собраны')

                        # Запись в лог
                        InsertLog('Все данные собраны')

                        isNotDone = False
                    elif isError == True:
                        print('Подключение не удалось')
                        traceback.print_exc()

                        # Запись в лог
                        startTime = datetime.datetime.now()
                        t_log = startTime.strftime('%d.%m.%Y %H:%M:%S')
                        # log = open('./src/Logs.txt', 'a+')
                        log = open('./src/Logs/' + str(YLog) + '/Logs_' + str(MLog) + '.txt', 'a+')
                        log.write(t_log + ' ## Подключение не удалось (ДРУГАЯ ОШИБКА)' + '\n')
                        traceback.print_exc(file=log)
                        log.close()

                        pass
                    else:
                        traceback.print_exc()
                        if isDebug == True:
                            traceback.print_exc()
                        print('Подключение через прокси не удалось')

                        # Запись в лог
                        startTime = datetime.datetime.now()
                        t_log = startTime.strftime('%d.%m.%Y %H:%M:%S')
                        log = open('./src/Logs/' + str(YLog) + '/Logs_' + str(MLog) + '.txt', 'a+')
                        log.write(t_log + ' ## Подключение через прокси не удалось (ДРУГАЯ ОШИБКА)' + '\n')
                        traceback.print_exc(file=log)
                        log.close()
                    pass

            except:
                isNotDone = False

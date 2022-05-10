import requests
from bs4 import BeautifulSoup
import random
import sys
import json
import datetime

# Для смены UserAgent
def GetRandomUserAgent ():
    with open("UserAgent.txt") as file:
        UserAgentAr = [row.strip() for row in file]

    Rand = random.randint(1, len(UserAgentAr))
    if Rand == len(UserAgentAr):
        Rand = Rand - 1
    return UserAgentAr[Rand]

def get_html (url, headers_HTML, params=None):
    r = requests.get(url, headers=headers_HTML, params=params)
    return r

def get_content(html, id):
    soup = BeautifulSoup(html.text, 'html.parser')
    items = soup.find('header-block-component')

    s = str(items)
    s = s.split('"id":"' + id + '"')[1]

    i_start = s.index('"inn":"') + 7
    i_end = i_start + 10

    INN = s[i_start:i_end].replace('"', '')

    i_start = s.index('"kpp":"') + 7
    i_end = i_start + 9

    KPP = s[i_start:i_end].replace('"', '')

    return INN, KPP

def get_Cust (url, headers, word):
    params = {"uuid": "d65a6e46-2e10-452c-98f9-048fcae44fcc", "dataVersion": "22.08.2019 14.15.25.520",
              "dsCode": "OOS_OOS_007_001_GridData", "paramSearch": word, "paramPeriod": "2021-11-30T00:00:00.000Z",
              "paramLocal": 0, "codeParam": 0, "_dc": "1638340474585", "page": 1, "start": 0, "limit": 50}

    r_cust = requests.get(url, params=params, headers=headers, verify=False, timeout=50)  # для организации

    status_code = r_cust.status_code

    if status_code != 200:
        return status_code, INN, KPP


# #карты
# L = random.choices(list(range(1, 14)), k=5)
# print(L)
# print(max([L.count(c) for c in L]))
# print([L.count(c) for c in L])

sys.exit()


word = 'ОБЛАСТНОЙ ПЕРИНАТАЛЬНЫЙ ЦЕНТР'
word = 'ГАУЗ ЯО КБ № 2'

#ПОИСК
word_search = 'лекарств'
Dtime_Start = '2021-11-24T00:00:00.000Z'
CurrentDate = '2021-12-02T00:00:00.000Z'
startT = datetime.datetime.now()  # Время для отслеживания работы всего парсера
CurrentDate = startT.strftime('%Y-%m-%dT00:00:00.000Z')  # Форматирование даты №1

print (CurrentDate)
#sys.exit()

url_search = 'https://zakupki.yarregion.ru/purchasesoflowvolume-asp/redirect/localhost/Data?uuid=8fff1ff7-e1da-4150-bdd7-2d3fc0709559&dataVersion=28.04.2021 07.57.58.612&dsCode=OOS_RCS_001_001_cardData&paramCompareDate=' + Dtime_Start + '&paramCurrentDate='+CurrentDate+'&paramSearchObject=' + word_search + '&paramSearchCustomer=&minPrice=&maxPrice=&purchaseNumParam=&paramSearchOKPD=&paramStateGroup=031,032,038,006&paramRuleActGroup=ISZ_RCS,ISZ_223&paramUndefinedValueGroup=Да,Нет&_dc=1638344702188'
#url_search = 'https://zakupki.yarregion.ru/purchasesoflowvolume-asp/redirect/localhost/Data?uuid=8fff1ff7-e1da-4150-bdd7-2d3fc0709559&dataVersion=28.04.2021 07.57.58.612&dsCode=OOS_RCS_001_001_cardData&paramCompareDate=2021-11-24T00:00:00.000Z&paramCurrentDate=2021-12-01T00:00:00.000Z&paramSearchObject=лекарств&paramSearchCustomer=&minPrice=&maxPrice=&purchaseNumParam=&paramSearchOKPD=&paramStateGroup=031,032,038,006&paramRuleActGroup=ISZ_RCS,ISZ_223&paramUndefinedValueGroup=Да,Нет&_dc=1638344702188'

#ЗАКАЗЧИК
url1 = 'https://zakupki.yarregion.ru/reestr-postavshhikov/redirect/localhost/Data'
url2 = 'https://zakupki.yarregion.ru/kartochka-zakazchika/redirect/localhost/Data?uuid=405e6bdb-4d57-4fb7-90ef-a1316cc62b27&dataVersion=22.08.2019 14.06.02.253&dsCode=OOS_OOS_003_003_grbs_pbs_Data&beginPeriodParam=2021-01-01T00:00:00.000Z&endPeriodParamCurr='+CurrentDate+'&_dc=1638869710881'


#Извещение
ID_Notif = '53209297'
url_Notif = 'https://zakupki.yarregion.ru/zakupki-malogo-obema-elektronnogo-magazina-detalno/redirect/localhost/Data?uuid=17199e28-6005-4dcd-9fb0-8381d5a0400c&dataVersion=30.04.2021 07.11.35.278&dsCode=gridData&paramPurchase=' + ID_Notif +'&_dc=1638354234456'
url_Notif = 'https://zakupki.yarregion.ru/zakupki-malogo-obema-elektronnogo-magazina-detalno/redirect/localhost/Data?uuid=17199e28-6005-4dcd-9fb0-8381d5a0400c&dataVersion=30.04.2021 07.11.35.278&dsCode=gridData&paramPurchase=53209297&_dc=1638354234456'
#Для НАИМЕНОВАНИЯ извещения
ID_Notif = '188974186'
url_Notif_Name = 'https://zakupki.yarregion.ru/zakupki-malogo-obema-elektronnogo-magazina-detalno/redirect/localhost/Data?uuid=17199e28-6005-4dcd-9fb0-8381d5a0400c&dataVersion=30.04.2021 07.11.35.278&dsCode=OOS_RKS_01_02_infoData&paramPurchase=' + ID_Notif +'&_dc=1638857878568'


#Победитель
Notif_Num='03712000192202100043'
ID_Notif='181688928'
url_Win = 'https://zakupki.yarregion.ru/zakupki-malogo-obema-elektronnogo-magazina-detalno/redirect/localhost/Data?uuid=17199e28-6005-4dcd-9fb0-8381d5a0400c&dataVersion=30.04.2021 07.11.35.278&dsCode=gridClsData&paramPurchase='+ID_Notif+'&purchaseNumParam='+Notif_Num+'&isUndefinedValue=Нет&_dc=1638354324692'

#'content-type': 'application/json',

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

word = 'ГОСУДАРСТВЕННОЕ КАЗЕННОЕ УЧРЕЖДЕНИЕ СОЦИАЛЬНОГО ОБСЛУЖИВАНИЯ ЯРОСЛАВСКОЙ ОБЛАСТИ ГАВРИЛОВ-ЯМСКИЙ ДЕТСКИЙ ДОМ-ИНТЕРНАТ ДЛЯ УМСТВЕННО ОТСТАЛЫХ ДЕТЕЙ'
word = 'ГОСУДАРСТВЕННОЕ ПРОФЕССИОНАЛЬНОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ЯРОСЛАВСКОЙ ОБЛАСТИ ЯРОСЛАВСКИЙ ПОЛИТЕХНИЧЕСКИЙ КОЛЛЕДЖ №24'

word = word.replace('ГБУЗ ЯО "ЯОКГВВ - МЦ ','государственное бюджетное учреждение здравоохранения Ярославской области "Ярославский областной клинический госпиталь ветеранов войн - международный центр по проблемам пожилых людей ')
word = word.replace('ГБУЗ ЯО','ГОСУДАРСТВЕННОЕ БЮДЖЕТНОЕ УЧРЕЖДЕНИЕ ЗДРАВООХРАНЕНИЯ ЯРОСЛАВСКОЙ ОБЛАСТИ').replace('КБ ','КЛИНИЧЕСКАЯ БОЛЬНИЦА ')

#word = '20000,00 - мг'
#word_split = word.split(' - ')[0].replace(',00', '').replace(',', '.')
#print('v=('+word_split+')')
#sys.exit()
#max_split = len(word_split)-1
#word = word_split[max_split-1] + ' ' + word_split[max_split]

# надо для организации (только params)
params = {"uuid": "d65a6e46-2e10-452c-98f9-048fcae44fcc", "dataVersion": "22.08.2019 14.15.25.520",
          "dsCode": "OOS_OOS_007_001_GridData", "paramSearch": word, "paramPeriod": "2021-11-30T00:00:00.000Z",
          "paramLocal": 0, "codeParam": 0, "_dc": "1638340474585", "page": 1, "start": 0, "limit": 50}


#data = json.dumps(params, ensure_ascii=False) # чтобы были русские буквы: , ensure_ascii=False
# encode json to utf-8
#encoded_data = data.encode('utf-8')

# print(params)
# n='15,1'
# print(n.replace(',00', '').replace(',', '.'))

#print(len(word_split))
#print(word_split)
#print(url_search)
#print(encoded_data)
#sys.exit()


#r00 = requests.get(url1, params=params, headers=headers, verify=False, timeout=50) #для организации

#r00_search = requests.post(url_search, headers=headers, verify=False, timeout=50) #для поиска на главной

r00_Notif = requests.post(url_Notif, headers=headers, verify=False, timeout=50) #для позиций извещения


#r00_Notif_Name = requests.post(url_Notif_Name, headers=headers, verify=False, timeout=50) #для НАИМЕНОВАНИЯ извещения

#r00_Win = requests.post(url_Win, headers=headers, verify=False, timeout=50) #для победителя


# Тоже работает для организации
#url1 = 'https://zakupki.yarregion.ru/reestr-postavshhikov/redirect/localhost/Data?uuid=d65a6e46-2e10-452c-98f9-048fcae44fcc&dataVersion=22.08.2019+14.15.25.520&dsCode=OOS_OOS_007_001_GridData&paramSearch=' + word.upper() + '&paramPeriod=2021-11-28T00%3A00%3A00.000Z&paramLocal=0&codeParam=0&_dc=1638193212849&page=1&start=0&limit=50'
#r00 = requests.post(url1, headers=headers, verify=False, timeout=50)

# для организации
# print(r00.status_code)
# r00 = r00.json()
# print(len(r00['data']))
# print(r00)
# #print(r00['data'][0][1],r00['data'][0][2])
# if len(r00['data']) == 0:
#     r00 = requests.get(url2, headers=headers, verify=False, timeout=50)  # для организации 2
#     if r00.status_code == 200:
#         r00 = r00.json()
#         org_len = len(r00['data'])
#         i_org = 0
#         INN = ''
#         while len(INN) == 0 and i_org < org_len:
#             if word in r00['data'][i_org][2]:
#                 INN = r00['data'][i_org][2].split('ИНН: ')[1]
#             i_org += 1
#     print(org_len,INN)
# sys.exit()

# для поиска на главной
# print(r00_search.status_code)
# r00_search = r00_search.json()
# #print(r00_search)
#
# print(r00_search['data'][0])
# print(r00_search['data'][13])
# print(r00_search['data'][13][8], r00_search['data'][13][8].replace(r'''\xa''', '').replace('₽', ''))
# total = r00_search['data']
# print(len(total))

# для извещения
print(r00_Notif.status_code)
r00_Notif = r00_Notif.json()
print(r00_Notif)
okpd = r00_Notif['data'][0][3].split(' - ')[0]
qt = r00_Notif['data'][0][4].split(' - ')[0]
qt_name = r00_Notif['data'][0][4].split(' - ')[1]

print(okpd, qt, qt_name)

# для НАИМЕНОВАНИЯ извещения
# print(r00_Notif_Name.status_code)
# r00_Notif_Name = r00_Notif_Name.json()
# print(r00_Notif_Name)

#WINNER
# print(r00_Win.status_code)
# r00_Win = r00_Win.json()
# print(r00_Win)

sys.exit()





#url1 = 'https://sberb2b.ru/request/supplier/preview/d16a2f83-78ec-43ee-91c8-fd8450121378'
url1 = 'https://sberb2b.ru/request/supplier/preview/a4537350-a7ab-4251-ab10-f335ccf1b7b5'

UserAgent = GetRandomUserAgent()

headers_HTML = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'user-agent': UserAgent
}



html = get_html(url1, headers_HTML)

if html.status_code == 200:
    print('200')
    INN, KPP = get_content (html, 'a4537350-a7ab-4251-ab10-f335ccf1b7b5')
    print(INN, KPP)

    soup = BeautifulSoup(html.text, 'html.parser')
    items = soup.find('header-block-component')

    #items = soup.find_all('"inn":"')
    #print(items)
    s = str(items)

    HaveFile = 0
    if '"file":' in s:
        HaveFile = 1

    print('HaveDocs: ' + str(HaveFile))
    s = s.split('"builded_string_from_deliveries":"')[1].split('","approval_status"')[0].rstrip()
    print (s)





    Org_ID_query = "SELECT top 1 Org_ID from [Cursor].[dbo].[Org] where INN = '0711056603_1' order by isnull(isZakupki,0) desc"
    Org_ID = parser_utils.select_query(Org_ID_query, login_sql, password_sql, 'Cursor', True)
    print('ooo',Org_ID)




    #f = '-'
    #print(f[-1])




    #if f[0:3] == 'таб':
    #     print('DA')

    # i_start = s.index('"inn":"') + 7
    # i_end = i_start + 10
    #
    # INN = s[i_start:i_end].replace('"','')
    #
    # i_start = s.index('"kpp":"') + 7
    # i_end = i_start + 9
    #
    # KPP = s[i_start:i_end].replace('"','')
    #
    # print(INN, KPP)



    #print(s.split('"id":"d16a2f83-78ec-43ee-91c8-fd8450121378"')[1])

    #print(s.index('"id":"d16a2f83-78ec-43ee-91c8-fd8450121378"'))
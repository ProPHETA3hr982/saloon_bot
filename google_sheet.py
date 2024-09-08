"""
Взаимодействие с Google Sheets
"""
from datetime import datetime, timedelta
from time import time
from threading import Lock
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from google.oauth2.service_account import Credentials
from retrying import retry
from cachetools import TTLCache
import gspread
from pytz import timezone

myscope = ["https://www.googleapis.com/auth/spreadsheets",
           "https://www.googleapis.com/auth/drive"]

# Временная зона
tz = timezone("Europe/Moscow")
# Название файла json ключа
creds = Credentials.from_service_account_file('telegram-bot-435009-1fb57c80f8c5.json', scopes=myscope)
client_main = gspread.Client(creds)
# Название таблицы
sh = client_main.open('telegram-bot-435009')
# Страницы таблицы, которые должны игнорироваться во избежание проблем
IGNOR_WORKSHEETS = ['Работники']
# Страница таблицы, на которой перечислены все действующие работники и услуги
NAME_SHEET_WORKERS = 'Работники'
# Названия основных колонок(очередность важна!)
NAME_COL_SERVICE = 'Услуга'
NAME_COL_MASTER = 'Мастер'

# Кэш листов с TTL (временем жизни) в 12 часов
CACHE_WORKSHEETS = TTLCache(maxsize=2, ttl=12 * 60 * 60)
# Кэш доступных дат для услуги-мастера с TTL (временем жизни) в 15 минут
CACHE_DAYS = TTLCache(maxsize=6, ttl=15 * 60)
# Lock для синхронизации доступа к словарям
lock = Lock()


# Функция для сериализации словаря в JSON-строку
def serialize_dict(dct: dict) -> str:
    """Сериализатор json"""
    return json.dumps(dct)


# Функция для десериализации JSON-строки в словарь
def deserialize_dict(json_str: str) -> dict:
    """Десериализатор json"""
    return json.loads(json_str)


def get_cache_days(service_name: str, master_name: str) -> Optional[list]:
    """
    Запрашивает свободные даты из кэша cashe_days

    :param service_name: Название услуги
    :param master_name: Имя мастера
    """
    if service_name in CACHE_DAYS:
        cached_value = CACHE_DAYS[service_name]
        cached_dict = deserialize_dict(cached_value)
        if master_name in cached_dict:
            return cached_dict[master_name]
    return None


def update_cache_days(service_name: str, master_name: str, available_dates: list) -> None:
    """
    Обновляет свободные даты для кэша cache_days

    :param service_name: Название услуги
    :param master_name: Имя мастера
    :param available_dates: Доступные даты
    """
    if master_name is None:
        master_name = 'null'

    if service_name in CACHE_DAYS:
        cached_value = CACHE_DAYS[service_name]
        cached_dict = deserialize_dict(cached_value)
        if master_name not in cached_dict:
            cached_dict[master_name] = available_dates
            cache_value = serialize_dict(cached_dict)
            CACHE_DAYS[service_name] = cache_value
    else:
        cache_value = serialize_dict({master_name: available_dates})
        CACHE_DAYS[service_name] = cache_value


@retry(wait_exponential_multiplier=3000, wait_exponential_max=3000)
def get_service_limits():
    """
    Получает лимиты записей на услуги из листа "Лимиты".

    :return: Словарь, где ключ - название услуги, значение - лимит.
    """
    with lock:
        limits_sheet = sh.worksheet("Лимиты")
        limits_data = limits_sheet.get_all_records()

    service_limits = {}
    for row in limits_data:
        service_limits[row["Услуга"].strip()] = int(row["Лимит"])

    return service_limits


@retry(wait_exponential_multiplier=3000, wait_exponential_max=3000)
def get_sheet_names() -> list:
    """
    Запрашивает все имена листов таблицы
    """
    # Проверяем, есть ли результат в кэше
    if 'worksheets' in CACHE_WORKSHEETS:
        return CACHE_WORKSHEETS['worksheets']
    with lock:
        worksheets = sh.worksheets()

    # Кэшируем результат
    CACHE_WORKSHEETS['worksheets'] = worksheets
    return worksheets


@retry(wait_exponential_multiplier=3000, wait_exponential_max=3000)
def get_cache_services() -> dict:
    """
    Запрашивает все услуги
    """
    if 'services' in CACHE_WORKSHEETS:
        return CACHE_WORKSHEETS['services']
    dct = {}
    with lock:
        ws = sh.worksheet(NAME_SHEET_WORKERS)
    for i in ws.get_all_records():
        dct[i[NAME_COL_SERVICE].strip()] = dct.get(i[NAME_COL_SERVICE].strip(), [])
        dct[i[NAME_COL_SERVICE].strip()].append(i[NAME_COL_MASTER].strip())

    CACHE_WORKSHEETS['services'] = dct
    return dct


def time_score(func):
    """Декоратор для трекинга времени выполнения функции"""

    def wrapper(*args, **kwargs):
        start = time()
        res = func(*args, **kwargs)
        print(f"---{func.__name__} = %s seconds ---" % round(time() - start, 2))
        return res

    return wrapper


class GoogleSheets:
    """Взаимодействие с GoogleSheet"""

    def __init__(self, client_id: str):
        # уникальный идентификатор для создания объекта
        self.client_id = client_id
        # доступные даты
        self.lst_currant_date = None
        # доступное время
        self.dct_currant_time = None
        # записи клиента
        self.lst_records = None

        self.name_service = None
        self.name_master = None
        self.date_record = None
        self.time_record = None

    def __str__(self):
        return f'Инфо о клиенте:\n' \
               f'{self.client_id=}\n' \
               f'{self.name_service=}\n' \
               f'{self.name_master=}\n' \
               f'{self.date_record=}\n' \
               f'{self.time_record=}'

    @retry(wait_exponential_multiplier=3000, wait_exponential_max=9000)
    def get_all_days(self) -> list:
        """Все доступные дни для записи на определенную услугу"""

        check = get_cache_days(self.name_service, self.name_master)
        if check:
            return check

        @retry(wait_exponential_multiplier=3000, wait_exponential_max=3000)
        def actual_date(sheet_obj, count_days=7) -> bool:
            """
            Проверяет по объекту листа актуальные даты для записи на ближайшие count_days дней,
            а также наличие свободного времени.

            :param sheet_obj: Объект листа (объект gspread)
            :param count_days: Количество ближайших дней для поиска (int)

            :return: True - есть доступные свободные слоты; False - все слоты заняты или нет актуальных дат
            """
            if sheet_obj.title in IGNOR_WORKSHEETS:
                return False
            try:
                date_sheet = datetime.strptime(sheet_obj.title.strip(), '%d.%m.%y').date()
            except Exception as ex:
                print(ex)
                print(sheet_obj.title, '- Добавьте лист в IGNOR_WORKSHEETS')
                return False
            date_today = datetime.now(tz=tz)
            if not date_today.date() <= date_sheet <= (datetime.now(tz=tz).date() + timedelta(days=count_days)):
                return False
            with lock:
                val = sheet_obj.get_all_records()
            for dct in val:

                if date_today.date() == date_sheet:
                    if (self.name_master is not None and
                        dct[NAME_COL_MASTER].strip() == self.name_master and
                        dct[NAME_COL_SERVICE].strip() == self.name_service) or \
                            (self.name_master is None and
                             dct[NAME_COL_SERVICE].strip() == self.name_service):
                        for k, v in dct.items():
                            if str(v).strip() == '' and date_today.time() < datetime.strptime(k, '%H:%M').time():
                                return sheet_obj.title
                    continue

                if (self.name_master is not None and dct[NAME_COL_MASTER].strip() == self.name_master and
                    dct[NAME_COL_SERVICE].strip() == self.name_service) \
                        or (self.name_master is None and dct[NAME_COL_SERVICE].strip() == self.name_service):
                    for k, v in dct.items():
                        if str(v).strip() == '':
                            return sheet_obj.title
            return False

        worksheet_all = get_sheet_names()

        with ThreadPoolExecutor(2) as executor:
            res = executor.map(actual_date, worksheet_all)
            res = list(filter(lambda x: type(x) is str, res))

        # Кэшируем результат
        update_cache_days(self.name_service, self.name_master, res)
        return res

    @retry(wait_exponential_multiplier=3000, wait_exponential_max=3000)
    def get_free_time(self) -> list:
        """
        Функция выгружает СВОБОДНОЕ ВРЕМЯ для определенной ДАТЫ с учетом лимитов.
        """
        try:
            with lock:
                all_val = sh.worksheet(self.date_record).get_all_records()
        except gspread.exceptions.WorksheetNotFound as not_found:
            print(not_found, self.date_record, '- Дата занята/не найдена')
            return []

        service_limits = get_service_limits()

        lst = []
        if self.date_record == datetime.now(tz=tz).strftime('%d.%m.%y'):
            for row in all_val:
                if (self.name_master is None and row[NAME_COL_SERVICE].strip() == self.name_service) or \
                        (self.name_master is not None and row[NAME_COL_SERVICE].strip() == self.name_service and
                         row[NAME_COL_MASTER].strip() == self.name_master):
                    for key_time, val_use in row.items():
                        if datetime.now(tz=tz).time() < datetime.strptime(key_time, '%H:%M').time():
                            if val_use.strip() == '':
                                lst.append(key_time.strip())
                            else:
                                try:
                                    records = json.loads(val_use)
                                    if len(records) < service_limits[self.name_service]:
                                        lst.append(key_time.strip())
                                except json.JSONDecodeError:
                                    # Если в ячейке не JSON, считаем ее занятой
                                    pass
        else:
            for row in all_val:
                if (self.name_master is None and row[NAME_COL_SERVICE].strip() == self.name_service) or \
                        (self.name_master is not None and row[NAME_COL_SERVICE].strip() == self.name_service and
                         row[NAME_COL_MASTER].strip() == self.name_master):
                    for key_time, val_use in row.items():
                        if val_use.strip() == '':
                            lst.append(key_time.strip())
                        else:
                            try:
                                records = json.loads(val_use)
                                if len(records) < service_limits[self.name_service]:
                                    lst.append(key_time.strip())
                            except json.JSONDecodeError:
                                # Если в ячейке не JSON, считаем ее занятой
                                pass

        if len(lst) > 0:
            lst = sorted(list(set(lst)))
        return lst

    @retry(wait_exponential_multiplier=3000, wait_exponential_max=3000)
    def set_time(self, client_record='', search_criteria='') -> bool:
        """
        Производит в таблицу запись/отмену клиента с учетом лимитов.
        """
        try:
            with lock:
                all_val = sh.worksheet(self.date_record).get_all_records()
        except gspread.exceptions.WorksheetNotFound as not_found:
            print(not_found, self.date_record, '- Дата занята/не найдена')
            return False

        row_num = 1
        for row in all_val:
            row_num += 1
            col_num = 0
            if (self.name_master is None and row[NAME_COL_SERVICE].strip() == self.name_service) or \
                    (self.name_master is not None and row[NAME_COL_SERVICE].strip() == self.name_service and
                     row[NAME_COL_MASTER].strip() == self.name_master):
                for key_time, val_use in row.items():
                    col_num += 1
                    if key_time.strip() == self.time_record:
                        if val_use.strip() == '':
                            # Ячейка пустая, записываем клиента
                            sh.worksheet(self.date_record).update_cell(row_num, col_num, json.dumps([client_record]))

                        else:
                            try:
                                records = json.loads(val_use)
                                service_limits = get_service_limits()
                                if len(records) < service_limits[self.name_service]:
                                    if search_criteria == '' and client_record not in records:
                                        records.append(client_record)
                                        sh.worksheet(self.date_record).update_cell(row_num, col_num,
                                                                                   json.dumps(records))
                                    elif search_criteria != '' and client_record in records:
                                        records.remove(client_record)
                                        if len(records) == 0:
                                            sh.worksheet(self.date_record).update_cell(row_num, col_num, '')
                                        else:
                                            sh.worksheet(self.date_record).update_cell(row_num, col_num,
                                                                                       json.dumps(records))
                                else:
                                    return False
                            except json.JSONDecodeError:
                                # Если в ячейке не JSON, считаем ее занятой
                                return False

                        if (self.lst_records and search_criteria == '') or (self.lst_records and client_record == ''):
                            record = [self.date_record, self.time_record, self.name_service, self.name_master]
                            if search_criteria == '':
                                self.lst_records.append(record)
                            else:
                                self.lst_records.remove(record)
                        return True
        return False

    @retry(wait_exponential_multiplier=3000, wait_exponential_max=3000)
    def get_record(self, client_record: str, count_days=7) -> list:
        """
        Находит все записи клиента на ближайшие <count_days> дней

        :param client_record: строка записи клиента.
        :param count_days: кол-во ближайших дней для поиска.

        :return: Список - формата: [Дата, Время, Название услуги, Имя мастера]
        """
        if self.lst_records:
            return self.lst_records

        @retry(wait_exponential_multiplier=3000, wait_exponential_max=3000)
        def check_record(sheet_obj) -> None:
            """Поиск брони клиента"""
            if sheet_obj.title in IGNOR_WORKSHEETS:
                return None
            try:
                date_sheet = datetime.strptime(sheet_obj.title, '%d.%m.%y')
            except Exception as ex:
                print(ex)
                print(sheet_obj.title, '- Добавьте лист в IGNOR_WORKSHEETS')
                return None
            date_today = datetime.now(tz=tz)
            if date_today.date() == date_sheet:
                with lock:
                    all_val = sheet_obj.get_all_records()
                lst_records.extend(
                    [sheet_obj.title.strip(), k.strip(), dct[NAME_COL_SERVICE].strip(), dct[NAME_COL_MASTER].strip()]
                    for dct in all_val
                    for k, v in dct.items()
                    if v != '' and client_record in json.loads(v)  # Изменено здесь
                )

            elif date_today.date() < date_sheet.date() <= (date_today + timedelta(days=count_days)).date():
                with lock:
                    all_val = sheet_obj.get_all_records()
                lst_records.extend(
                    [sheet_obj.title.strip(), k.strip(), dct[NAME_COL_SERVICE].strip(), dct[NAME_COL_MASTER].strip()]
                    for dct in all_val
                    for k, v in dct.items()
                    if v != '' and client_record in json.loads(v)  # Изменено здесь
                )

        lst_records = []
        with ThreadPoolExecutor(2) as executor:
            executor.map(check_record, sh.worksheets())
        self.lst_records = lst_records
        return lst_records

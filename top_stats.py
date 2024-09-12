import json
import logging
import sys
from datetime import datetime, timedelta
from time import sleep
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectionError
from tqdm import tqdm
from urllib3.util.retry import Retry

system_arguments = sys.argv[1:]
time_delta = (
    timedelta(days=7) if len(system_arguments) == 1 and system_arguments[0] == 'week' else
    timedelta(days=31) if len(system_arguments) == 1 and system_arguments[0] == 'month' else None
)

pd.options.display.max_rows = 1500
pd.options.display.max_columns = 1500

level = logging.INFO
fmt = '[%(levelname)s] %(asctime)s - %(message)s'
logging.basicConfig(level=level, format=fmt)

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

date_today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
date_month_ago = date_today.replace(hour=0, minute=0, second=0, microsecond=0) - time_delta
formatted_date_month_ago = date_month_ago.strftime("%Y-%m-%dT%H:%M:%S%z")
date_to = date_today.astimezone().replace(microsecond=0).isoformat()
date_from = date_month_ago.astimezone().replace(microsecond=0).isoformat()

login = "ПетровИС".encode('utf-8')
password = "5gA7owua"
Client_ID = 'taxi/park/d64bab312a6944fe9a4563c060499850'
API_KEY = 'JpgaNIdMsRZBSfvZukuPgBFMorwJtmMdnDnsFo'
Park_ID = 'd64bab312a6944fe9a4563c060499850'

df = pd.DataFrame(
    columns=[
        'driver_name',
        'orders',
        'hours',
        'cash',
        'cashless',
        'platform_commission',
        'park_commission'
    ]
)

driver_hours_headers = {
    'User-Agent': 'Mozilla/5.0 \
    (Macintosh; Intel Mac OS X 10_9_3) \
    AppleWebKit/537.36 (KHTML, like Gecko) \
    Chrome/35.0.1916.47 Safari/537.36',
    "Accept-Language": "ru", "X-Park-ID": Park_ID, "X-Client-ID": Client_ID, "X-API-Key": API_KEY
}
driver_profile_headers = {
    'User-Agent': 'Mozilla/5.0 \
    (Macintosh; Intel Mac OS X 10_9_3) \
    AppleWebKit/537.36 (HTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36',
    "Accept-Language": "ru",
    "X-Park-ID": Park_ID,
    "X-Client-ID": Client_ID,
    "X-API-Key": API_KEY
}
driver_order_list_headers = {
    "X-Client-ID": Client_ID,
    "X-API-Key": API_KEY
}
driver_transaction_list_headers = {
    "Accept-Language": "ru",
    "X-Client-ID": Client_ID,
    "X-API-Key": API_KEY
}


def parse_json_t(url, body):
    result = None
    json_body = json.dumps(body, ensure_ascii=False).encode('utf8')
    try:
        result = requests.post(url, json_body, auth=HTTPBasicAuth(login, password))
    except ConnectionError:
        logging.error("Произошла ошибка соединения с сервером API.")
    except Exception:
        logging.error("Произошла непредвиденная ошибка.")
    try:
        return result.json()
    except UnboundLocalError:
        return []


def parse_json(url, headers, body):
    result = None
    if sys.version_info < (3,):
        def u(x):
            try:
                return x.encode("utf8")
            except UnicodeDecodeError:
                return x
    else:
        def u(x):
            if type(x) == type(b''):
                return x.decode('utf8')
            else:
                return x
    json_body = json.dumps(body, ensure_ascii=False).encode('utf8')

    try:
        result = requests.get(url, json_body, headers=headers)
    except ConnectionError:
        logging.error("Произошла ошибка соединения с сервером API.")
    except Exception:
        logging.error("Произошла непредвиденная ошибка.")

    return result.json()


def parse_driver_profile(url: str, headers: dict) -> Optional[dict]:
    driver_profile = None
    try:
        driver_profile = session.get(driver_profile_url, headers=driver_profile_headers).json()
        car_id = driver_profile["car_id"]
    except ConnectionError as error:
        logging.error(f"Connection error: {error}")
    except Exception as error:
        logging.error(f"Driver not found")

    return driver_profile


logging.info("Sending request to the drivers API.")
sleep(5)
logging.info("Waiting for response from the drivers API...")
URL = "https://taksi.0nalog.com:1703/globus-taxi/hs/Driver/v1/Get"
driver_taxi_body = {'DetailBalance': False}
driver = parse_json_t(URL, driver_taxi_body)
driver = pd.DataFrame.from_dict(driver)
driver = driver.loc[(driver.DismissalDate > '2023-08-01T19:55:22') | (driver.Status == 'Работает')]
driver = driver[driver['DefaultID'] != '']
driver = driver[driver['CarDepartment'] != 'Краснодар']
driver = driver[driver['CarDepartment'] != 'Глобус']
driver = driver[driver['CarDepartment'] != 'МСК']
driver = driver[driver['CarDepartment'] != '']

logging.info("Start to process the drivers data.")

for driver_profile_id in tqdm(driver['DefaultID']):
    transaction_list = None
    order_list = None
    orders_count: int = 0
    driver_car_id = None
    driver_fio: str = ""
    cash: float = 0.0
    cashless: float = 0.0
    total_summ: float = 0.0
    rounded_hours: int = 0
    platform_commission: float = 0.0
    park_commission: float = 0.0
    partner_fee = [
        "partner_bonus_fee",
        "partner_ride_fee",
        "partner_subscription_fee",
        "platform_other_gas_fleet_fee"
    ]
    platform_fee = [
        "platform_bonus_fee",
        "platform_ride_fee",
        "platform_ride_vat"
    ]

    driver_profile_url = (
        f'https://fleet-api.taxi.yandex.net/v2/parks/contractors/driver-profile?'
        f'contractor_profile_id={driver_profile_id}'
    )
    driver_hours_url = (
        f'https://fleet-api.taxi.yandex.net/v2/parks/contractors/supply-hours?'
        f'contractor_profile_id={driver_profile_id}&'
        f'period_from={date_from}&'
        f'period_to={date_to}'
    )
    driver_orders_url = 'https://fleet-api.taxi.yandex.net/v1/parks/orders/list'
    driver_transaction_list_url = 'https://fleet-api.taxi.yandex.net/v2/parks/driver-profiles/transactions/list'

    try:
        driver_profile = parse_driver_profile(driver_profile_url, driver_profile_headers)

        if driver_profile:
            person_info = driver_profile.get("person").get("full_name")
            last_name = person_info.get("last_name", "")
            first_name = person_info.get("first_name", "")
            middle_name = person_info.get("middle_name", "")
            driver_fio = f"{last_name} {first_name} {middle_name}"
            driver_car_id = driver_profile["car_id"]

        order_list_body = {
            "limit": 500,
            "query": {
                "park": {
                    "car": {
                        "id": driver_car_id
                    },
                    "driver_profile": {
                        "id": driver_profile_id
                    },
                    "id": Park_ID,
                    "order": {
                        "booked_at": {
                            "from": date_from,
                            "to": date_to
                        },

                        "statuses": [
                            "complete"
                        ],

                    }
                }
            }
        }
        transaction_list_body = {
            "limit": 1000,
            "query": {
                "park": {
                    "driver_profile": {
                        "id": driver_profile_id
                    },
                    "id": Park_ID,
                    "transaction": {
                        "category_ids": [
                            "partner_bonus_fee",
                            "partner_ride_fee",
                            "partner_subscription_fee",
                            "platform_other_gas_fleet_fee",
                            "platform_bonus_fee",
                            "platform_ride_fee",
                            "platform_ride_vat"
                        ],
                        "event_at": {
                            "from": date_from,
                            "to": date_to
                        }
                    }
                }
            }
        }

        pars_driver_hours = parse_json(driver_hours_url, driver_hours_headers, {})
        if 'supply_duration_seconds' in pars_driver_hours:
            sec = pars_driver_hours['supply_duration_seconds']
            hours = sec / 3600
            rounded_hours = round(hours)

        # Parse order list
        order_list_json_body = json.dumps(order_list_body, ensure_ascii=False).encode("utf8")

        try:
            try: # DELETE THIS
                order_list = session.post(
                    driver_orders_url,
                    order_list_json_body,
                    headers=driver_order_list_headers
                ).json()
                if order_list:
                    orders_count += len(order_list["orders"])
            except Exception as e:
                # logging.error(f"error: {e}")
                pass
        except ConnectionError as error:
            logging.error(f'Failed to get driver order list, error: {error}')
        for order in order_list["orders"]:
            if order["payment_method"] == "cash":
                cash += float(order["price"])
            if order["payment_method"] == "cashless":
                cashless += float(order["price"])
            total_summ += float(order["price"])
        if "cursor" in order_list:
            cursor = True
            while cursor:
                order_list_body["cursor"] = order_list["cursor"]
                order_list_json_body = json.dumps(order_list_body, ensure_ascii=False).encode("utf8")
                try:
                    order_list = session.post(
                        driver_orders_url,
                        order_list_json_body,
                        headers=driver_order_list_headers
                    ).json()
                    if order_list:
                        orders_count += len(order_list["orders"]) # Check
                        for order in order_list["orders"]:
                            if order["payment_method"] == "cash":
                                cash += float(order["price"])
                            if order["payment_method"] == "cashless":
                                cashless += float(order["price"])
                            total_summ += float(order["price"])
                        if "cursor" in order_list:
                            cursor = True
                        else:
                            cursor = False
                except ConnectionError as error:
                    logging.error(f'Failed to get driver order list, error: {error}')

        # Parse transaction list
        transaction_list_json_body = json.dumps(transaction_list_body, ensure_ascii=False).encode("utf8")

        try:
            transaction_list = session.post(
                driver_transaction_list_url,
                transaction_list_json_body,
                headers=driver_transaction_list_headers
            ).json()
        except ConnectionError as error:
            logging.error(f'Failed to get driver transaction, error: {error}')
        for transaction in transaction_list["transactions"]:
            if transaction["category_id"] in partner_fee:
                park_commission += float(transaction["amount"])
            if transaction["category_id"] in platform_fee:
                platform_commission += float(transaction["amount"])
        if "cursor" in transaction_list:
            cursor = True
            while cursor:
                transaction_list_body["cursor"] = transaction_list["cursor"]
                transaction_list_json_body = json.dumps(transaction_list_body, ensure_ascii=False).encode("utf8")
                try:
                    transaction_list = session.post(
                        driver_transaction_list_url,
                        transaction_list_json_body,
                        headers=driver_transaction_list_headers
                    ).json()
                    for transaction in transaction_list["transactions"]:
                        if transaction["category_id"] in partner_fee:
                            park_commission += float(transaction["amount"])
                        if transaction["category_id"] in platform_fee:
                            platform_commission += float(transaction["amount"])
                    if "cursor" in transaction_list:
                        cursor = True
                    else:
                        cursor = False
                except ConnectionError as error:
                    logging.error(f'Failed to get driver transaction, error: {error}')

        df.loc[len(df)] = {
            'driver_name': driver_fio,
            'orders': orders_count,
            'hours': rounded_hours,
            'cash': cash,
            'cashless': cash,
            'platform_commission': platform_commission,
            'park_commission': park_commission
        }
    except Exception as e:
        # logging.error(f"Failed to parse driver with driver id: {driver_profile_id}")
        pass

a = pd.read_excel('top_pokazateli.xlsx')

if 'sec' in a.columns:
    a = a.drop('sec', axis=1)
if 'date' in a.columns:
    a = a.drop('date', axis=1)
if 'Unnamed: 0' in a.columns:
    a = a.drop('Unnamed: 0', axis=1)
if 'driver_id' in a.columns:
    a = a.drop('driver_id', axis=1)

df = pd.concat([a, df])
df.to_excel('top_pokazateli.xlsx', index=False)

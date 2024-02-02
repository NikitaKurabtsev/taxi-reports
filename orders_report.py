import json
import logging
import os
import sys
from datetime import datetime, timedelta
from time import sleep
from typing import List, Optional, Tuple

import pandas as pd
import requests
from dateutil import parser
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException
from tqdm import tqdm
from urllib3.util.retry import Retry

load_dotenv()

LOGIN = os.getenv("LOGIN").encode('utf-8')
PASSWORD = os.getenv("PASSWORD")

PARK_1_HEADERS = {
    'park_id': os.getenv("PARK_ID_1"),
    'client_id': os.getenv("CLIENT_ID_1"),
    'api_key': os.getenv("API_KEY_1")
}
PARK_2_HEADERS = {
    'park_id': os.getenv("PARK_ID_2"),
    'client_id': os.getenv("CLIENT_ID_2"),
    'api_key': os.getenv("API_KEY_2")
}

# park commission
PARTNER_FEE = [
    "partner_bonus_fee",
    "partner_ride_fee",
    "partner_subscription_fee",
    "platform_other_gas_fleet_fee"
    ]

# logger configurations
level = logging.INFO
fmt = '[%(levelname)s] %(asctime)s - %(message)s'
logging.basicConfig(level=level, format=fmt)

# session configurations
session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

# date range input configurations
system_arguments = sys.argv[1:]

if len(system_arguments) == 1 and system_arguments[0] == 'day':
    time_delta = timedelta(days=1)
elif len(system_arguments) == 1 and system_arguments[0] == 'week':
    time_delta = timedelta(days=7)
elif len(system_arguments) == 1 and system_arguments[0] == 'month':
    time_delta = timedelta(days=31)
else:
    raise ValueError("Выберите аргумент для вызова: day, week или month.")

# date configurations
date_today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
date_month_ago = date_today.replace(hour=0, minute=0, second=0, microsecond=0) - time_delta
formatted_date_month_ago = date_month_ago.strftime("%Y-%m-%dT%H:%M:%S%z")
date_to = date_today.astimezone().replace(microsecond=0).isoformat()
date_from = date_month_ago.astimezone().replace(microsecond=0).isoformat()

# pandas configurations
pd.options.display.max_rows = 1500
pd.options.display.max_columns = 1500

DATAFRAME = pd.DataFrame(
    columns=[
        'date',
        'driver_car',
        'driver_name',
        'duration',
        'commission',
    ]
)


def get_driver_profile_url(driver_id) -> str:
    profile_url: str = (
        f'https://fleet-api.taxi.yandex.net/v2/parks/contractors/driver-profile?'
        f'contractor_profile_id={driver_id}'
    )
    return profile_url


def get_driver_orders_url() -> str:
    orders_url: str = 'https://fleet-api.taxi.yandex.net/v1/parks/orders/list'

    return orders_url


def get_driver_transactions_url() -> str:
    driver_transaction_list_url: str = (
        'https://fleet-api.taxi.yandex.net/v2/parks/orders/transactions/list'
    )

    return driver_transaction_list_url


def get_drivers_1c_url() -> str:
    drivers_url = "https://taksi.0nalog.com:1703/globus-taxi/hs/Driver/v1/Get"

    return drivers_url


def get_driver_orders_body(
    driver_car_id: str,
    driver_id: str,
    park_id: str,
    from_date: str,
    to_date: str
) -> dict:
    orders_list_body = {
        "limit": 500,
        "query": {
            "park": {
                "car": {
                    "id": driver_car_id
                },
                "driver_profile": {
                    "id": driver_id
                },
                "id": park_id,
                "order": {
                    "booked_at": {
                        "from": from_date,
                        "to": to_date
                    },
                    "statuses": [
                        "complete"
                    ],

                }
            }
        }
    }

    return orders_list_body


def get_driver_transactions_body(
    park_id: str,
    driver_order_id: str,
    from_date: str,
    to_date: str
) -> dict:
    transactions_list_body = {
        "query": {
            "park": {
                "id": park_id,
                "order": {
                    "ids": [
                        driver_order_id
                    ]
                },
                "transaction": {
                    "event_at": {
                        "from": from_date,
                        "to": to_date
                    }
                }
            }
        }
    }

    return transactions_list_body


def get_api_headers(**kwargs) -> Tuple[dict, dict, dict]:
    park_id = kwargs.get("park_id", None)
    client_id = kwargs.get("client_id", None)
    api_key = kwargs.get("api_key", None)

    driver_headers = {
        'User-Agent': 'Mozilla/5.0 \
        (Macintosh; Intel Mac OS X 10_9_3) \
        AppleWebKit/537.36 (HTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36',
        "Accept-Language": "ru",
        "X-Park-ID": park_id,
        "X-Client-ID": client_id,
        "X-API-Key": api_key
    }
    driver_order_headers = {
        "X-Client-ID": client_id,
        "X-API-Key": api_key
    }
    driver_transaction_headers = {
        "Accept-Language": "ru",
        "X-Client-ID": client_id,
        "X-API-Key": api_key
    }

    return driver_headers, driver_order_headers, driver_transaction_headers


def prepare_data_frame(
    drivers_data: dict,
    from_date: str
) -> pd.DataFrame:
    drivers_df = pd.DataFrame.from_dict(drivers_data)
    drivers_df = drivers_df.loc[(drivers_df.DismissalDate > from_date) | (drivers_df.Status == 'Работает')]
    drivers_df = drivers_df[drivers_df['DefaultID'] != '']
    drivers_df = drivers_df[drivers_df['CarDepartment'] != 'Краснодар']
    drivers_df = drivers_df[drivers_df['CarDepartment'] != 'Глобус']
    drivers_df = drivers_df[drivers_df['CarDepartment'] != 'МСК']
    drivers_df = drivers_df[drivers_df['CarDepartment'] != '']

    return drivers_df


def process_data_frame(
    data_frame: pd.DataFrame,
    report_date: datetime,
    driver_car: str,
    driver: str,
    duration: str,
    driver_commission: str
) -> None:
    """Creates a new row in a Pandas DataFrame."""
    new_row = {
        'date': report_date,
        'driver_car': driver_car,
        'driver_name': driver,
        'duration': duration,
        'commission': driver_commission,
    }
    data_frame.loc[len(data_frame)] = new_row


def fetch_1c_drivers(
    url: str,
    login: bytes,
    password: str
) -> dict:
    """Fetches driver's from 1c database."""
    drivers_1c: Optional[dict] = None
    body: dict = {'DetailBalance': False}
    json_body = json.dumps(body, ensure_ascii=False).encode('utf8')

    try:
        raw_response = requests.post(url, json_body, auth=HTTPBasicAuth(login, password))
        raw_response.raise_for_status()
        drivers_1c = raw_response.json()
    except RequestException as error:
        logging.error(f"API connection error: {error}")
    except Exception as error:
        logging.error(f"1C API error: {error}.")
        logging.error(f"Check the 1C API login and password in the script.")

    return drivers_1c


def fetch_driver_profile(
    url: str,
    headers: dict,
    default_id: str
) -> Tuple[dict, str]:
    """Fetches driver's profile from the Yandex API."""
    driver_profile_data: Optional[dict] = None
    driver_car_id: Optional[str] = None

    try:
        raw_response = session.get(url, headers=headers)
        raw_response.raise_for_status()
        driver_profile_data = raw_response.json()
        driver_car_id = driver_profile_data.get("car_id", "")
    except RequestException as error:
        logging.error(f"Yandex drivers profile API connection error: {error}")
    except Exception as error:
        logging.error(f"Driver with DefaultID: {default_id} not found, error: {error}")

    return driver_profile_data, driver_car_id


def fetch_driver_order_list(
    url: str,
    body: dict,
    headers: dict
) -> List[dict]:
    """Fetches driver's orders from the Yandex API."""
    orders = []
    try:
        while True:
            json_body = json.dumps(body, ensure_ascii=False).encode("utf8")
            raw_response = session.post(url, json_body, headers=headers)
            raw_response.raise_for_status()
            response = raw_response.json()
            orders.extend(response["orders"])
            if "cursor" in response and len(response["cursor"]) > 1:
                body["cursor"] = response["cursor"]
            else:
                break
    except RequestException as error:
        logging.error(f"Yandex driver orders API request error : {error}")
    except Exception as error:
        logging.error(f"Yandex driver orders API error: {error}")

    return orders


def fetch_driver_transactions_list(
    url: str,
    body: dict,
    headers: dict
) -> list[dict]:
    """Fetches driver's orders from the Yandex API."""
    transactions = []
    try:
        while True:
            json_body = json.dumps(body, ensure_ascii=False).encode("utf8")
            raw_response = session.post(url, json_body, headers=headers)
            raw_response.raise_for_status()
            response = raw_response.json()
            transactions.extend(response["transactions"])
            if "cursor" in response and len(response["cursor"]) > 1:
                body["cursor"] = response["cursor"]
            else:
                break
    except RequestException as error:
        logging.error(f"Yandex driver transactions API request error : {error}")
    except Exception as error:
        logging.error(f"Yandex driver transactions API error: {error}")

    return transactions


def process_driver_fio(data: dict) -> str:
    """
    Process person full name.

    Returns last name, first name and middle.
    """
    driver: dict = data.get("person").get("full_name")
    last_name = driver.get("last_name", "")
    first_name = driver.get("first_name", "")
    middle_name = driver.get("middle_name", "")

    return f"{last_name} {first_name} {middle_name}"


def process_driver_car(data: dict) -> str:
    """
    Process driver's car information.

    Returns car model and car number.
    """
    car_data = data.get("car", "")
    car_model = car_data.get("brand_model", "")
    car_number = car_data.get("license", "").get("number", "")

    return f"{car_model} {car_number}"


def process_order_duration(
    start_time_str: str,
    end_time_str: str
) -> str:
    """
    Calculate the duration of an order and return
    hours, minutes, and seconds.
    """
    start_time = parser.isoparse(start_time_str)
    end_time = parser.isoparse(end_time_str)

    duration = end_time - start_time

    hours, remainder = divmod(duration.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return f"{hours}:{minutes}:{seconds}"


def process_driver_commission(commission_amount: str) -> str:
    """
    Turns the transaction commission into a float number
    and rounds to one decimal place.
    """
    amount = float(commission_amount)
    formatted_amount = f"{amount:.1f}".replace(".", ",")

    return formatted_amount


def process_park(
    drivers_data_frame: pd.DataFrame,
    park_headers: dict,
    from_date: str,
    to_date: str,
    date_now: datetime
) -> None:
    park_id = park_headers.get("park_id", "")
    driver_headers, order_headers, transaction_headers = get_api_headers(**park_headers)

    for driver_profile_id in tqdm(drivers_data_frame['DefaultID']):
        driver_profile_url = get_driver_profile_url(driver_profile_id)
        driver_orders_url = get_driver_orders_url()
        driver_transactions_url = get_driver_transactions_url()

        driver_profile, car_id = fetch_driver_profile(
            driver_profile_url,
            driver_headers,
            driver_profile_id
        )

        orders_body = get_driver_orders_body(
            car_id,
            driver_profile_id,
            park_id,
            from_date,
            to_date
        )
        order_list = fetch_driver_order_list(
            driver_orders_url,
            orders_body,
            order_headers
        )

        for order in order_list:
            if order["status"] == "complete":
                order_id = order.get("id", "")
                order_booked = order.get("booked_at")
                order_ended = order.get("ended_at")

                transactions_body = get_driver_transactions_body(
                    park_id,
                    order_id,
                    from_date,
                    to_date
                )
                transaction_list = fetch_driver_transactions_list(
                    driver_transactions_url,
                    transactions_body,
                    transaction_headers
                )

                for transaction in transaction_list:
                    transaction_order_id = transaction.get("order_id", "")
                    transaction_category = transaction.get("category_id", "")
                    transaction_commission = transaction.get("amount", "")

                    if transaction_order_id == order_id and transaction_category in PARTNER_FEE:
                        fio = process_driver_fio(driver_profile)
                        car = process_driver_car(order)
                        order_duration = process_order_duration(order_booked, order_ended)
                        commission = process_driver_commission(transaction_commission)

                        process_data_frame(
                            DATAFRAME,
                            date_now,
                            car,
                            fio,
                            order_duration,
                            commission
                        )
            else:
                pass


logging.info("Sending request to the drivers API.")
sleep(5)
logging.info("Waiting for response from the drivers API...")

drivers_1c_url = get_drivers_1c_url()
drivers_dict = fetch_1c_drivers(drivers_1c_url, LOGIN, PASSWORD)
drivers = prepare_data_frame(drivers_dict, date_from)

logging.info("Start to process the drivers data.")

process_park(drivers, PARK_1_HEADERS, date_from, date_to, date_today)
process_park(drivers, PARK_2_HEADERS, date_from, date_to, date_today)


try:
    previous_report = pd.read_excel('orders_report.xlsx')
    new_report = pd.concat([previous_report, DATAFRAME])
    new_report.to_excel('orders_report.xlsx', index=False)
except FileNotFoundError:
    DATAFRAME.to_excel('orders_report.xlsx', index=False)

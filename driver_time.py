import json
from datetime import datetime, timedelta

import pandas as pd
import requests
from requests.exceptions import ConnectionError
from tqdm import tqdm
from requests.auth import HTTPBasicAuth

pd.options.display.max_rows = 1500
pd.options.display.max_columns = 1500

raw_prev_date = datetime.now() - timedelta(days=1)
raw_date_time = datetime.now()
date_today = raw_date_time.date()

date_time = raw_date_time.isoformat()
prev_date = raw_prev_date.isoformat()

login = "".encode("utf-8")
password = ""
Client_ID = ""
API_KEY = ""
Park_ID = ""
login = ""
password = ""
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36",
    "Accept-Language": "ru",
    "X-Park-ID": Park_ID,
    "X-Client-ID": Client_ID,
    "X-API-Key": API_KEY,
}


def parse_drivers_1c(url, body):
    json_body = json.dumps(body, ensure_ascii=False).encode("utf8")
    result = {}

    try:
        result = requests.post(url, json_body, auth=HTTPBasicAuth(login, password))
    except ConnectionError:
        print("Произошла ошибка соединения с сервером API.")
    except:
        print("Произошла непредвиденная ошибка.")

    return result.json()


def parse_driver_hours(url, headers, body):
    json_body = json.dumps(body, ensure_ascii=False).encode("utf8")
    result = {}

    try:
        result = requests.get(url, json_body, headers=headers)
    except ConnectionError:
        print("Произошла ошибка соединения с сервером API.")
    except:
        print("Произошла непредвиденная ошибка.")

    return result.json()


def parse_driver_profile(url, headers, body):
    json_body = json.dumps(body, ensure_ascii=False).encode("utf8")
    result = {}

    try:
        result = requests.get(url, json_body, headers=headers)
    except ConnectionError:
        print("Произошла ошибка соединения с сервером API.")
    except:
        print("Произошла непредвиденная ошибка.")

    return result.json()


def parse_driver_car(url, headers, body):
    json_body = json.dumps(body, ensure_ascii=False).encode("utf8")
    result = {}

    try:
        result = requests.get(url, json_body, headers=headers)
    except ConnectionError:
        print("Произошла ошибка соединения с сервером API.")
    except:
        print("Произошла непредвиденная ошибка.")

    return result.json()


def process_driver_fio(driver_data: dict) -> str:
    driver_fio: dict = driver_data.get("person").get("full_name")
    last_name = driver_fio.get("last_name", "")
    first_name = driver_fio.get("first_name", "")
    middle_name = driver_fio.get("middle_name", "")

    return f"{last_name} {first_name} {middle_name}"


df = pd.DataFrame(columns=["date", "driver_fio", "hours", "car_vin"])

URL = "https://taksi.0nalog.com:1703/globus-taxi/hs/Driver/v1/Get"
body = {"DetailBalance": False}
driver = parse_drivers_1c(URL, {"DetailBalance": False})
# TODO: Fix the driver slice

driver = pd.DataFrame.from_dict(driver)
driver = driver.loc[
    (
        (driver.DismissalDate > "2023-08-01T19:55:22")
        | (driver.Status.isin(["Работает", "На линии"]))
    )
]
driver = driver[driver["DefaultID"] != ""]
driver = driver[driver["CarDepartment"] != "Краснодар"]
driver = driver[driver["CarDepartment"] != "Глобус"]
driver = driver[driver["CarDepartment"] != "МСК"]
driver = driver[driver["CarDepartment"] != ""]

for x in tqdm(driver["DefaultID"]):
    vin = ""
    d_id = x
    try:
        driver_profile_url = f"https://fleet-api.taxi.yandex.net/v2/parks/contractors/driver-profile?contractor_profile_id={d_id}"
        url = (
            "https://fleet-api.taxi.yandex.net/v2/parks/contractors/supply-hours?contractor_profile_id="
            + d_id
            + "&period_from="
            + prev_date
            + "&period_to="
            + date_time
        )
        driver_hours = parse_driver_hours(url, headers, {})
        if (
            "supply_duration_seconds" in driver_hours
            and driver_hours["supply_duration_seconds"] > 0
        ):
            driver_profile = parse_driver_profile(driver_profile_url, headers, {})
            car_id = driver_profile.get("car_id", "")
            driver_car_url = f"https://fleet-api.taxi.yandex.net/v2/parks/vehicles/car?vehicle_id={car_id}"
            car_vin = parse_driver_car(driver_car_url, headers, {})
            vin = car_vin["vehicle_specifications"]["vin"]
            driver_fio = process_driver_fio(driver_profile)
            sec = driver_hours["supply_duration_seconds"]
            hours = sec / 3600
            rounded_hours = round(hours)
            df.loc[len(df)] = {
                "date": date_today,
                "driver_fio": driver_fio,
                "hours": rounded_hours,
                "car_vin": vin,
            }
            print(f" - driver: {driver_fio}, hours: {rounded_hours}")
            print(date_today)
        else:
            pass
    except Exception:
        print(f"Can't parse the driver with id: {d_id}")

update_path = r"C:\Users\Administrator\OneDrive\time_report.xlsx"
refresh_path = r"C:\FTP\time_report.xlsx"
bounded_data_frame = df

try:
    previous_report = pd.read_excel(update_path)
    update_report = pd.concat([previous_report, bounded_data_frame])
    update_report.to_excel(update_path, index=False)
except FileNotFoundError:
    bounded_data_frame.to_excel(update_path, index=False)

refresh_report = bounded_data_frame
refresh_report.to_excel(refresh_path, index=False)

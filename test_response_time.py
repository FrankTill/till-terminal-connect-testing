import time
import csv
from datetime import datetime
import os
import threading
import requests
from dotenv import load_dotenv


load_dotenv()
HOST = os.getenv("HOST")
API_KEY = os.getenv("API_KEY")
POSTBACK_URL = os.getenv("POSTBACK_URL")
HEADERS = {"x-api-key": API_KEY}
REPORT_FILE_NAME = os.getenv("REPORT_FILE_NAME")
MID_TID_FILE_NAME = os.getenv("MID_TID_FILE_NAME")
NO_OF_TESTS = int(os.getenv("NO_OF_TESTS"))


def read_mid_tid(filename) -> list:
    available_mid_tid = []
    with open(filename, mode="r", encoding="utf-8") as file:
        reader = csv.reader(file)
        next(reader)
        for line in reader:
            available_mid_tid.append(line)
    return available_mid_tid


def write_csv(filename, data):
    write_header = False
    if not os.path.exists(filename):
        write_header = True
    with open(filename, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        if write_header:
            writer.writerow(
                ["Endpoint", "Start Time", "End Time", "Wait Time (seconds)"]
            )
        writer.writerow(data)


def create_intent(mid) -> str:
    url = f"{HOST}/devices/merchant/{mid}/intent/payment"
    merchant_ref = datetime.now().strftime("%Y%m%d%H%M%S")
    payload = {
        "subTotal": 1,
        "tip": 0,
        "tax": 0,
        "merchantReference": merchant_ref,
        "manualCardEntry": False,
        "postbackUrl": POSTBACK_URL,
    }
    resp = requests.post(url=url, headers=HEADERS, json=payload, timeout=60)
    if resp.ok:
        return resp.json().get("intentId", "")
    else:
        print(f"Create: {resp.status_code} {resp.text}")
        return ""


def process_intent(mid, tid, intent_id) -> str:
    url = f"{HOST}/devices/merchant/{mid}/intent/{intent_id}/process"
    payload = {"tid": tid}
    resp = requests.post(url=url, headers=HEADERS, json=payload, timeout=60)
    return str(resp.status_code)


def get_intent(mid, intent_id) -> str:
    url = f"{HOST}/devices/merchant/{mid}/intent/{intent_id}"
    resp = requests.get(url=url, headers=HEADERS)
    if resp.ok:
        return resp.json().get("status", "")
    else:
        print(f"Get: {resp.status_code} {resp.text}")
        return ""


def get_terminal(mid, tid) -> str:
    url = f"{HOST}/devices/merchant/{mid}/terminals/{tid}"
    resp = requests.get(url=url, headers=HEADERS)
    if resp.ok:
        try:
            terminal_status = resp.json()["terminal"]["status"]["connectivity"]
        except KeyError:
            terminal_status = ""
    else:
        terminal_status = ""
    return terminal_status


def make_a_txn(filename, available_mid_tid) -> None:
    [mid, tid] = available_mid_tid.pop(0)
    intent_id = ""
    create_attempt = 0
    while intent_id == "" and create_attempt < 10:
        start_time = datetime.now()
        start_time_unix = time.time()
        intent_id = create_intent(mid)
        end_time = datetime.now()
        end_time_unix = time.time()
        wait_time = end_time_unix - start_time_unix
        write_csv(
            filename,
            [
                f"create-{intent_id}",
                start_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                wait_time,
            ],
        )
        create_attempt += 1
        time.sleep(5)
    if intent_id == "":
        available_mid_tid.append([mid, tid])
        return
    if intent_id != "":
        process_attempt = 0
        process_result = ""
        while process_result not in ["201", "400", "422"] and process_attempt < 5:
            start_time = datetime.now()
            start_time_unix = time.time()
            process_result = process_intent(mid, tid, intent_id)
            end_time = datetime.now()
            end_time_unix = time.time()
            wait_time = end_time_unix - start_time_unix
            write_csv(
                filename,
                [
                    f"process-{intent_id}",
                    start_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                    end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                    wait_time,
                ],
            )
            process_attempt += 1
            time.sleep(5)
        if process_result not in ["201", "400", "422"]:
            available_mid_tid.append([mid, tid])
            return
        elif process_result in ["201", "400"]:
            get_attempt = 0
            intent_status = "PROCESSING"
            while (
                intent_status not in ["CANCELLED", "FAILED", "COMPLETED"]
                and get_attempt < 15
            ):
                start_time = datetime.now()
                start_time_unix = time.time()
                intent_status = get_intent(mid, intent_id)
                end_time = datetime.now()
                end_time_unix = time.time()
                wait_time = end_time_unix - start_time_unix
                write_csv(
                    filename,
                    [
                        f"get-{intent_id}",
                        start_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        wait_time,
                    ],
                )
                get_attempt += 1
                time.sleep(10)
            if intent_status in ["CANCELLED", "FAILED", "COMPLETED"]:
                available_mid_tid.append([mid, tid])

        # not logging this request as WU's not using it
        elif process_result == "422":
            terminal_status = ""
            get_t_attempt = 0
            while terminal_status != "AVAILABLE" and get_t_attempt < 15:
                terminal_status = get_terminal(mid, tid)
                get_t_attempt += 1
                time.sleep(10)
            if terminal_status == "AVAILABLE":
                available_mid_tid.append([mid, tid])


def main() -> None:
    available_mid_tid = read_mid_tid(MID_TID_FILE_NAME)
    not_available_times = 0

    for i in range(NO_OF_TESTS):
        if available_mid_tid:
            print(f"Making request {i+1}")
            threads = []

            for _ in range(min(len(available_mid_tid), len(available_mid_tid))):
                thread = threading.Thread(
                    target=make_a_txn, args=(REPORT_FILE_NAME, available_mid_tid)
                )
                thread.start()
                threads.append(thread)

            # Ensure all threads complete before proceeding
            for thread in threads:
                thread.join()
        elif not_available_times < 10:
            print("No available TIDs")
            not_available_times += 1
            time.sleep(60)
        else:
            break


if __name__ == "__main__":
    main()

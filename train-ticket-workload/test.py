# These helper function comes from train-ticket-auto-query, so does query_one_and_cancel and so on.

import logging
import os
import time

import requests
from query_scaffold.scenarios import query_and_preserve, query_and_collect
from query_scaffold.atomic_queries import _cancel_one_order, _query_high_speed_ticket, _query_normal_ticket, _query_assurances, _query_food, _query_contacts, _query_orders, _collect_one_order, _pay_one_order
from query_scaffold.utils import random_boolean, random_phone, random_str
from query_scaffold.queries import Query
import random

q = Query("http://localhost:8080")

base_address="http://localhost:8080"
logger = logging.getLogger("query_and_preserve")
# The UUID of user fdse_microservice is that
uuid = "4d2a46c7-71cb-4cf1-b5bb-b68406d9da6f"
date = time.strftime("%Y-%m-%d", time.localtime())

def query_one_and_cancel(headers, uuid="4d2a46c7-71cb-4cf1-b5bb-b68406d9da6f"):
    """
    查询order并取消order
    :param uuid:
    :param headers:
    :return:
    """
    pairs = _query_orders(headers=headers, types=tuple([0, 1]))
    pairs2 = _query_orders(headers=headers, types=tuple([0, 1]), query_other=True)

    if not pairs and not pairs2:
        return

    pairs = pairs + pairs2

    # (orderId, tripId) pair
    pair = random.choice(pairs)

    order_id =_cancel_one_order(order_id=pair[0], uuid=uuid, headers=headers)
    if not order_id:
        return

    print(f"{order_id} queried and canceled")

def query_order_and_pay(headers, pairs):
    """
    查询Order并付款未付款Order
    :return:
    """

    # (orderId, tripId) pair
    if len(pairs) is 0:
        return
    pair = random.choice(pairs)

    order_id = _pay_one_order(pair[0], pair[1], headers=headers)
    if not order_id:
        return

    print(f"{order_id} queried and paid")

def query_and_collect_ticket(headers):

    pairs = _query_orders(headers=headers, types=tuple([1]))
    pairs2 = _query_orders(headers=headers, types=tuple([1]), query_other=True)

    if not pairs and not pairs2:
        return

    pairs = pairs + pairs2

    # (orderId, tripId)
    pair = random.choice(pairs)

    order_id = _collect_one_order(order_id=pair[0], headers=headers)
    if not order_id:
        return

    print(f"{order_id} queried and collected")


def query_and_preserve(headers):
    """
    1. 查票（随机高铁或普通）
    2. 查保险、Food、Contacts
    3. 随机选择Contacts、保险、是否买食物、是否托运
    4. 买票
    :return:
    """
    start = ""
    end = ""
    trip_ids = []
    PRESERVE_URL = ""

    high_speed = random_boolean()
    if high_speed:
        start = "Shang Hai"
        end = "Su Zhou"
        high_speed_place_pair = (start, end)
        trip_ids = _query_high_speed_ticket(place_pair=high_speed_place_pair, headers=headers, time=date)
        PRESERVE_URL = f"{base_address}/api/v1/preserveservice/preserve"
    else:
        start = "Shang Hai"
        end = "Nan Jing"
        other_place_pair = (start, end)
        trip_ids = _query_normal_ticket(place_pair=other_place_pair, headers=headers, time=date)
        PRESERVE_URL = f"{base_address}/api/v1/preserveotherservice/preserveOther"
    print(PRESERVE_URL)

    _ = _query_assurances(headers=headers)
    food_result = _query_food(headers=headers)
    contacts_result = _query_contacts(headers=headers)

    base_preserve_payload = {
        "accountId": uuid,
        "assurance": "0",
        "contactsId": "",
        "date": date,
        "from": start,
        "to": end,
        "tripId": ""
    }

    trip_id = random.choice(trip_ids)
    base_preserve_payload["tripId"] = trip_id

    need_food = random_boolean()
    if need_food:
        logger.info("need food")
        food_dict = random.choice(food_result)
        base_preserve_payload.update(food_dict)
    else:
        logger.info("not need food")
        base_preserve_payload["foodType"] = "0"

    need_assurance = random_boolean()
    if need_assurance:
        base_preserve_payload["assurance"] = 1

    contacts_id = random.choice(contacts_result)
    base_preserve_payload["contactsId"] = contacts_id

    # 高铁 2-3
    seat_type = random.choice(["2", "3"])
    base_preserve_payload["seatType"] = seat_type

    need_consign = random_boolean()
    if need_consign:
        consign = {
            "consigneeName": random_str(),
            "consigneePhone": random_phone(),
            "consigneeWeight": random.randint(1, 10),
            "handleDate": date
        }
        base_preserve_payload.update(consign)

    print("payload:" + str(base_preserve_payload))

    print(f"choices: preserve_high: {high_speed} need_food:{need_food}  need_consign: {need_consign}  need_assurance:{need_assurance}")

    res = requests.post(url=PRESERVE_URL,
                        headers=headers,
                        json=base_preserve_payload)

    print(res.json())
    if res.json()["data"] != "Success":
        raise Exception(res.json() + " not success")


# for i in range(1000):
#     q.login()
#     qqq.query_and_preserve()

import time
import psutil

def monitor_system():
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_usage = psutil.virtual_memory().percent
    return cpu_usage, memory_usage

flag = False

for i in range(1000000):
    try:
        cpu_usage, memory_usage = monitor_system()
        if cpu_usage > 80 or memory_usage > 99.5:
            print(f"High usage detected: CPU {cpu_usage}%, Memory {memory_usage}%")
            time.sleep(5)
            continue
        if memory_usage > 99:
            flag = True
        else:
            flag = False

        q.login()
        query_and_preserve(q.session.headers)
        q.query_cheapest()
        q.query_food()
        q.query_assurances()
        query_and_collect_ticket(q.session.headers)
        orders = q.query_orders()
        query_order_and_pay(q.session.headers, orders)
        query_one_and_cancel(q.session.headers, orders)
    except Exception as e:
        print(e)
        time.sleep(5)



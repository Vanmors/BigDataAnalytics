#!/usr/bin/env python3

import csv
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 20240913
ROOT = Path(__file__).resolve().parent
CRAFT_MARKET_DIR = ROOT / "craft_market"
EXTERNAL_SOURCE_DIR = ROOT / "external_source"

N_CRAFTSMANS = 5
N_PRODUCTS = 20
N_CUSTOMERS = 10
N_ORDERS = 30
N_ORDER_LOG = 10
N_ACTIVITY_LOG = 10
N_RESEARCH = 10
N_ACTIONS = 8

PERIOD_START = date(2026, 7, 1)
PERIOD_END = date(2026, 9, 30)

PRODUCT_TYPES = [
    "jewelry", "ceramics", "textile", "wood", "leather",
    "candles", "soap", "toys", "paintings", "glass",
]
CITIES = ["Saint Petersburg", "Moscow", "Kazan", "Novosibirsk", "Yekaterinburg"]
FIRST_NAMES = ["Anna", "Ivan", "Olga", "Petr", "Maria", "Sergey", "Elena", "Dmitry", "Irina", "Nikolay"]
LAST_NAMES = ["Ivanov", "Petrov", "Sidorov", "Smirnov", "Kuznetsov", "Popov", "Volkov", "Sokolov"]
MIDDLE_NAMES = ["Ivanovich", "Petrovich", "Sergeevna", "Nikolaevna", "Dmitrievich", "Olegovna"]
STREETS = ["Lenina", "Sadovaya", "Nevsky", "Gagarina", "Pushkina", "Mira"]

ORDER_STATUSES = ["created", "in progress", "delivery", "done"]
STATUS_WEIGHTS = [10, 15, 10, 65]  # most of the orders are completed


def write_csv(path: Path, header: list[str], rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"{path.relative_to(ROOT.parent)}: {len(rows)} rows")


def random_date(rnd: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=rnd.randint(0, (end - start).days))


def random_datetime(rnd: random.Random, start: date, end: date) -> str:
    day = random_date(rnd, start, end)
    moment = datetime(day.year, day.month, day.day) + timedelta(seconds=rnd.randint(0, 86399))
    return moment.isoformat(sep=" ")


def random_uuid(rnd: random.Random) -> str:
    return str(uuid.UUID(int=rnd.getrandbits(128), version=4))


def full_name(rnd: random.Random) -> str:
    return f"{rnd.choice(LAST_NAMES)} {rnd.choice(FIRST_NAMES)} {rnd.choice(MIDDLE_NAMES)}"


def address(rnd: random.Random) -> str:
    return f"{rnd.choice(CITIES)}, {rnd.choice(STREETS)} st., {rnd.randint(1, 120)}"


def person(rnd: random.Random, pid: int, born_from: date, born_to: date, domain: str) -> tuple:
    name = full_name(rnd)
    email = f"{name.split()[0].lower()}{pid}@{domain}"
    return pid, name, address(rnd), random_date(rnd, born_from, born_to).isoformat(), email


def main() -> None:
    rnd = random.Random(SEED)

    craftsmans = [
        person(rnd, cid, date(1970, 1, 1), date(2000, 12, 31), "craft.example.com")
        for cid in range(1, N_CRAFTSMANS + 1)
    ]
    write_csv(
        CRAFT_MARKET_DIR / "craft_market_craftsmans.csv",
        ["craftsman_id", "craftsman_name", "craftsman_address", "craftsman_birthday", "craftsman_email"],
        craftsmans,
    )

    customers = [
        person(rnd, cid, date(1960, 1, 1), date(2006, 12, 31), "example.com")
        for cid in range(1, N_CUSTOMERS + 1)
    ]
    write_csv(
        CRAFT_MARKET_DIR / "craft_market_customers.csv",
        ["customer_id", "customer_name", "customer_address", "customer_birthday", "customer_email"],
        customers,
    )

    products = {}
    for pid in range(1, N_PRODUCTS + 1):
        ptype = rnd.choice(PRODUCT_TYPES)
        products[pid] = (f"Handmade item #{pid}", f"Handmade {ptype} item, unique piece", ptype)

    prices: dict[tuple[int, int], int] = {}
    for craftsman_id in range(1, N_CRAFTSMANS + 1):
        for product_id in rnd.sample(range(1, N_PRODUCTS + 1), rnd.randint(4, 10)):
            prices[(craftsman_id, product_id)] = rnd.randint(500, 25000)
    catalog = list(prices)

    orders = []
    for order_id in range(1, N_ORDERS + 1):
        craftsman_id, product_id = rnd.choice(catalog)
        created = random_date(rnd, PERIOD_START, PERIOD_END)
        status = rnd.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
        completed = created + timedelta(days=rnd.randint(2, 45)) if status == "done" else None
        if completed is not None and completed > PERIOD_END:
            status, completed = "delivery", None
        completed = completed.isoformat() if completed else ""
        orders.append((
            order_id,
            product_id,
            craftsman_id,
            rnd.randint(1, N_CUSTOMERS),
            created.isoformat(),
            completed,
            status,
            *products[product_id],
            prices[(craftsman_id, product_id)],
        ))
    write_csv(
        CRAFT_MARKET_DIR / "craft_market_orders.csv",
        ["order_id", "product_id", "craftsman_id", "customer_id", "order_created_date",
         "order_completion_date", "order_status", "product_name", "product_description",
         "product_type", "product_price"],
        orders,
    )

    write_csv(
        CRAFT_MARKET_DIR / "craft_market_masters_products.csv",
        ["craftsman_id", "craftsman_name", "craftsman_address", "craftsman_birthday", "craftsman_email",
         "product_id", "product_name", "product_description", "product_type", "product_price"],
        [(*craftsmans[cid - 1], pid, *products[pid], price) for (cid, pid), price in sorted(prices.items())],
    )
    write_csv(
        CRAFT_MARKET_DIR / "craft_market_orders_customers.csv",
        ["order_id", "craftsman_id", "product_id", "order_created_date", "order_completion_date",
         "order_status", "customer_id", "customer_name", "customer_address", "customer_birthday",
         "customer_email"],
        [(oid, cid, pid, created, completed, status, *customers[customer_id - 1])
         for oid, pid, cid, customer_id, created, completed, status, *_ in orders],
    )

    generate_external_source(customers, products, prices)

    print("\nDone. Next: `make up`, then `make seed`, then run the DAG in the Airflow UI.")


def generate_external_source(
    customers: list[tuple],
    products: dict[int, tuple],
    prices: dict[tuple[int, int], int],
) -> None:
    # Task 2: raw files for the external_source schema.

    rnd = random.Random(SEED + 2)
    city_ids = {city: i for i, city in enumerate(CITIES, start=1)}
    item_prices: dict[int, int] = {}
    for (_, product_id), price in prices.items():
        item_prices[product_id] = min(price, item_prices.get(product_id, price))
    items = sorted(item_prices)

    order_log = []
    for row_id in range(1, N_ORDER_LOG + 1):
        customer_id, name, customer_address, *_ = rnd.choice(customers)
        last_name, first_name, _ = name.split()
        city_name = customer_address.split(",")[0]
        item_id = rnd.choice(items)
        quantity = rnd.randint(1, 5)
        order_log.append((
            row_id,
            random_uuid(rnd),
            random_datetime(rnd, PERIOD_START, PERIOD_END),
            city_ids[city_name],
            city_name,
            customer_id,
            first_name,
            last_name,
            item_id,
            products[item_id][0],
            quantity,
            quantity * item_prices[item_id],
        ))
    order_log.sort(key=lambda r: r[2])
    write_csv(
        EXTERNAL_SOURCE_DIR / "user_order_log.csv",
        ["id", "uniq_id", "date_time", "city_id", "city_name", "customer_id", "first_name",
         "last_name", "item_id", "item_name", "quantity", "payment_amount"],
        [(i, *r[1:]) for i, r in enumerate(order_log, start=1)],
    )

    activity_log = []
    for _ in range(N_ACTIVITY_LOG):
        activity_log.append((
            random_uuid(rnd),
            random_datetime(rnd, PERIOD_START, PERIOD_END),
            rnd.randint(1, N_ACTIONS),
            rnd.randint(1, len(customers)),
            rnd.randint(1, 10),
        ))
    activity_log.sort(key=lambda r: r[1])
    write_csv(
        EXTERNAL_SOURCE_DIR / "user_activity_log.csv",
        ["id", "uniq_id", "date_time", "action_id", "customer_id", "quantity"],
        [(i, *r) for i, r in enumerate(activity_log, start=1)],
    )

    research = []
    for _ in range(N_RESEARCH):
        sales_qty = rnd.randint(0, 40)
        research.append((
            random_date(rnd, PERIOD_START, PERIOD_END).isoformat(),
            rnd.randint(1, len(PRODUCT_TYPES)),
            rnd.randint(1, len(CITIES)),
            sales_qty,
            sales_qty * rnd.randint(500, 25000),
        ))
    research.sort()
    write_csv(
        EXTERNAL_SOURCE_DIR / "customer_research.csv",
        ["date_id", "category_id", "geo_id", "sales_qty", "sales_amt"],
        research,
    )


if __name__ == "__main__":
    main()

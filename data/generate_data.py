#!/usr/bin/env python3
"""Generate test data for both tasks. Standard library only - no pip install needed.

    python3 data/generate_data.py

Writes:
    data/source/*.csv            - OLTP tables of task 1 (loaded by `make seed`)
    data/external_source/*.csv   - the three files of task 2 (loaded by the Airflow DAG)

The generator is deterministic (fixed random seed), so re-running it does not change the data.
"""

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 20240913
ROOT = Path(__file__).resolve().parent
SOURCE_DIR = ROOT / "source"
EXTERNAL_DIR = ROOT / "external_source"

N_MASTERS = 30
N_PRODUCTS = 120
N_CLIENTS = 200
N_ORDERS = 4000

PERIOD_START = date(2024, 1, 1)
PERIOD_END = date(2024, 6, 30)

PRODUCT_TYPES = [
    "jewelry", "ceramics", "textile", "wood", "leather",
    "candles", "soap", "toys", "paintings", "glass",
]
CITIES = [
    (1, "Saint Petersburg"), (2, "Moscow"), (3, "Kazan"),
    (4, "Novosibirsk"), (5, "Yekaterinburg"),
]
FIRST_NAMES = ["Anna", "Ivan", "Olga", "Petr", "Maria", "Sergey", "Elena", "Dmitry", "Irina", "Nikolay"]
LAST_NAMES = ["Ivanov", "Petrov", "Sidorov", "Smirnov", "Kuznetsov", "Popov", "Volkov", "Sokolov"]
MIDDLE_NAMES = ["Ivanovich", "Petrovich", "Sergeevna", "Nikolaevna", "Dmitrievich", "Olegovna"]
STREETS = ["Lenina", "Sadovaya", "Nevsky", "Gagarina", "Pushkina", "Mira"]

ORDER_STATUSES = ["created", "in_progress", "delivery", "done"]
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


def full_name(rnd: random.Random) -> str:
    return f"{rnd.choice(LAST_NAMES)} {rnd.choice(FIRST_NAMES)} {rnd.choice(MIDDLE_NAMES)}"


def address(rnd: random.Random) -> str:
    city = rnd.choice(CITIES)[1]
    return f"{city}, {rnd.choice(STREETS)} st., {rnd.randint(1, 120)}"


def generate_task1(rnd: random.Random) -> None:
    masters = [
        (
            mid,
            full_name(rnd),
            f"+7{rnd.randint(9000000000, 9999999999)}",
            address(rnd),
            random_date(rnd, date(1970, 1, 1), date(2000, 12, 31)).isoformat(),
        )
        for mid in range(1, N_MASTERS + 1)
    ]
    write_csv(SOURCE_DIR / "master.csv", ["master_id", "name", "phone", "address", "birthday"], masters)

    products = [
        (
            pid,
            f"Handmade item #{pid}",
            f"Handmade {rnd.choice(PRODUCT_TYPES)} item, unique piece",
            rnd.choice(PRODUCT_TYPES),
        )
        for pid in range(1, N_PRODUCTS + 1)
    ]
    write_csv(SOURCE_DIR / "product.csv", ["product_id", "name", "description", "type"], products)

    # Each master makes a handful of products; the pair (master, product) sets the price.
    master_products: list[tuple] = []
    catalog: dict[int, list[int]] = {}
    mp_id = 1
    for mid in range(1, N_MASTERS + 1):
        made = rnd.sample(range(1, N_PRODUCTS + 1), rnd.randint(4, 10))
        catalog[mid] = made
        for pid in made:
            master_products.append((mp_id, mid, pid, round(rnd.uniform(500, 25000), 2)))
            mp_id += 1
    write_csv(SOURCE_DIR / "master_product.csv", ["id", "master_id", "product_id", "price"], master_products)

    clients = []
    for cid in range(1, N_CLIENTS + 1):
        name = full_name(rnd)
        login = name.split()[0].lower()
        clients.append((
            cid,
            name,
            f"+7{rnd.randint(9000000000, 9999999999)}",
            address(rnd),
            random_date(rnd, date(1960, 1, 1), date(2006, 12, 31)).isoformat(),
            f"{login}{cid}@example.com",
        ))
    write_csv(SOURCE_DIR / "client.csv", ["client_id", "name", "phone", "address", "birthday", "email"], clients)

    orders = []
    for oid in range(1, N_ORDERS + 1):
        master_id = rnd.randint(1, N_MASTERS)
        product_id = rnd.choice(catalog[master_id])
        client_id = rnd.randint(1, N_CLIENTS)
        order_date = random_date(rnd, PERIOD_START, PERIOD_END)
        status = rnd.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
        delivery_date = (order_date + timedelta(days=rnd.randint(2, 45))).isoformat() if status == "done" else ""
        orders.append((oid, master_id, product_id, client_id, order_date.isoformat(), delivery_date, status))
    write_csv(
        SOURCE_DIR / "orders.csv",
        ["order_id", "master_id", "product_id", "client_id", "order_date", "delivery_date", "status"],
        orders,
    )


def generate_task2(rnd: random.Random) -> None:
    """The three files of task 2 - exactly the columns listed in the document."""
    order_statuses = []
    for rid in range(1, 3001):
        city_id, city_name = rnd.choice(CITIES)
        moment = datetime.combine(random_date(rnd, PERIOD_START, PERIOD_END), datetime.min.time()) \
            + timedelta(seconds=rnd.randint(0, 86399))
        quantity = rnd.randint(1, 5)
        order_statuses.append((
            rid,
            moment.isoformat(sep=" "),
            city_id,
            city_name,
            rnd.randint(1, N_CLIENTS),
            rnd.choice(FIRST_NAMES),
            rnd.choice(LAST_NAMES),
            rnd.randint(1, N_PRODUCTS),
            f"Handmade item #{rnd.randint(1, N_PRODUCTS)}",
            quantity,
            round(quantity * rnd.uniform(500, 25000), 2),
        ))
    write_csv(
        EXTERNAL_DIR / "order_statuses.csv",
        ["id", "date_time", "city_id", "city_name", "client_id", "first_name", "last_name",
         "item_id", "item_name", "quantity", "payment_amount"],
        order_statuses,
    )

    user_behavior = []
    for rid in range(1, 5001):
        moment = datetime.combine(random_date(rnd, PERIOD_START, PERIOD_END), datetime.min.time()) \
            + timedelta(seconds=rnd.randint(0, 86399))
        user_behavior.append((
            rid,
            moment.isoformat(sep=" "),
            rnd.randint(1, 6),          # behavior_id: view / add to cart / checkout / ...
            rnd.randint(1, N_CLIENTS),
            rnd.randint(1, 5),
        ))
    write_csv(
        EXTERNAL_DIR / "user_behavior.csv",
        ["id", "date_time", "behavior_id", "user_id", "quantity"],
        user_behavior,
    )

    marketing_research = []
    day = PERIOD_START
    while day <= PERIOD_END:
        for category_id in range(1, len(PRODUCT_TYPES) + 1):
            qty = rnd.randint(0, 60)
            marketing_research.append((day.isoformat(), category_id, qty, round(qty * rnd.uniform(500, 8000), 2)))
        day += timedelta(days=1)
    write_csv(
        EXTERNAL_DIR / "marketing_research.csv",
        ["date_id", "category_id", "sales_qty", "sales_amt"],
        marketing_research,
    )


def main() -> None:
    rnd = random.Random(SEED)
    generate_task1(rnd)
    generate_task2(rnd)
    print("\nDone. Next: `make ddl` then `make seed`, and run the DAGs in the Airflow UI.")


if __name__ == "__main__":
    main()

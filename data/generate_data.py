#!/usr/bin/env python3
"""Generate test data for task 1. Standard library only - no pip install needed.

    python3 data/generate_data.py

Writes data/craft_market/*.csv - the three source3.craft_market_* tables (loaded by `make seed`).
The generator is deterministic (fixed random seed), so re-running it does not change the data.
"""

import csv
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 20240913
ROOT = Path(__file__).resolve().parent
CRAFT_MARKET_DIR = ROOT / "craft_market"

N_CRAFTSMANS = 30
N_PRODUCTS = 120
N_CUSTOMERS = 200
N_ORDERS = 4000

PERIOD_START = date(2024, 1, 1)
PERIOD_END = date(2024, 6, 30)

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

    # product_id -> (name, description, type)
    products = {}
    for pid in range(1, N_PRODUCTS + 1):
        ptype = rnd.choice(PRODUCT_TYPES)
        products[pid] = (f"Handmade item #{pid}", f"Handmade {ptype} item, unique piece", ptype)

    # Each craftsman makes a handful of products; the pair (craftsman, product) sets the price.
    prices: dict[tuple[int, int], int] = {}
    for craftsman_id in range(1, N_CRAFTSMANS + 1):
        for product_id in rnd.sample(range(1, N_PRODUCTS + 1), rnd.randint(4, 10)):
            prices[(craftsman_id, product_id)] = rnd.randint(500, 25000)
    catalog = list(prices)

    # The order row carries the product itself, as in source3.craft_market_orders.
    orders = []
    for order_id in range(1, N_ORDERS + 1):
        craftsman_id, product_id = rnd.choice(catalog)
        created = random_date(rnd, PERIOD_START, PERIOD_END)
        status = rnd.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
        completed = (created + timedelta(days=rnd.randint(2, 45))).isoformat() if status == "done" else ""
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

    print("\nDone. Next: `make up`, then `make seed`, then run the DAG in the Airflow UI.")


if __name__ == "__main__":
    main()

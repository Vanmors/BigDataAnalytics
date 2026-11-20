#!/usr/bin/env python3
"""Generate deterministic social network data for lab 2. Standard library only - no pip install needed.

    python3 data/generate_data.py                                  # small demo set
    python3 data/generate_data.py --people 50 --groups 8 --messages 500

Writes data/social_network/*.csv:
    people.csv   - h_people + s_person_info
    groups.csv   - h_groups + s_group_info + l_chat_owners (owner_id)
    messages.csv - h_chats + s_chat_info + l_person_chat (msg_from) + l_groups (group_id)

A message is either private (msg_to is set, group_id is empty) or a group message (group_id is set, msg_to is empty).
The model has no group membership link, so members exist only here: group messages are written by group members.
The generator is deterministic (fixed random seed), so re-running it with the same sizes does not change the data.
"""

import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 20261111
ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "social_network"

DEFAULT_PEOPLE = 10
DEFAULT_GROUPS = 3
DEFAULT_MESSAGES = 10
MAX_GROUP_MEMBERS = 15
GROUP_MESSAGE_SHARE = 0.6
SILENT_PEOPLE_SHARE = 0.1  # registered but never write - "active users" in dm_demography must differ from all users
DEAD_GROUP_SHARE = 0.15  # groups that stop getting messages long before the period end

PERIOD_START = date(2026, 10, 1)
PERIOD_END = date(2026, 10, 31)
BIRTH_START = date(1960, 1, 1)
BIRTH_END = date(2010, 12, 31)

FIRST_NAMES = ["Anna", "Ivan", "Olga", "Petr", "Maria", "Sergey", "Elena", "Dmitry", "Irina", "Nikolay",
               "Alexey", "Daria", "Pavel", "Ksenia", "Mikhail", "Sofia"]
LAST_NAMES = ["Ivanov", "Petrov", "Sidorov", "Smirnov", "Kuznetsov", "Popov", "Volkov", "Sokolov",
              "Lebedev", "Kozlov", "Novikov", "Morozov"]
COUNTRIES = ["Russia", "Kazakhstan", "Belarus", "Armenia", "Georgia", "Serbia", "Turkey", "Germany"]
COUNTRY_WEIGHTS = [50, 12, 10, 6, 6, 5, 6, 5]
PHONE_CODES = {"Russia": "+7", "Kazakhstan": "+7", "Belarus": "+375", "Armenia": "+374",
               "Georgia": "+995", "Serbia": "+381", "Turkey": "+90", "Germany": "+49"}
EMAIL_DOMAINS = ["mail.ru", "gmail.com", "yandex.ru", "outlook.com"]

GROUP_TOPICS = ["Data Engineering", "Hiking", "Board Games", "Photography", "Cooking", "Python",
                "Running Club", "Book Club", "Startups", "Movies", "Chess", "Travel", "Music", "Gardening"]
GROUP_SUFFIXES = ["Chat", "Community", "Friends", "Talks", "Hub"]
GROUP_STATUSES = ["active", "archived", "blocked"]
GROUP_STATUS_WEIGHTS = [80, 15, 5]

MESSAGE_STATUSES = ["sent", "delivered", "read"]
MESSAGE_STATUS_WEIGHTS = [5, 15, 80]
MESSAGE_TEXTS = ["Hi!", "How are you?", "See you tomorrow", "Thanks a lot", "Good morning",
                 "Did you see the news?", "Let's meet at 7", "Sounds good", "I agree", "Who is in?",
                 "Great photo", "Happy birthday!", "Sorry, I am late", "Check this link", "Ok"]

# Messages are written mostly in the evening and less on weekends - visible in dm_messages_daily.
HOUR_WEIGHTS = [1, 1, 1, 1, 1, 1, 2, 4, 6, 7, 7, 7, 8, 7, 7, 7, 8, 9, 10, 12, 12, 10, 6, 3]
WEEKDAY_WEIGHTS = [10, 10, 10, 10, 9, 6, 5]


def write_csv(path: Path, header: list[str], rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"{path.relative_to(ROOT.parent)}: {len(rows)} rows")


def random_date(rnd: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=rnd.randint(0, (end - start).days))


def random_message_time(rnd: random.Random, start: date, end: date) -> datetime:
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    day = rnd.choices(days, weights=[WEEKDAY_WEIGHTS[d.weekday()] for d in days])[0]
    hour = rnd.choices(range(24), weights=HOUR_WEIGHTS)[0]
    return datetime(day.year, day.month, day.day, hour, rnd.randint(0, 59), rnd.randint(0, 59))


def at_least_one(total: int, share: float) -> int:
    return max(1, round(total * share))


def generate_people(rnd: random.Random, n_people: int) -> list[tuple]:
    people = []
    for person_id in range(1, n_people + 1):
        first, last = rnd.choice(FIRST_NAMES), rnd.choice(LAST_NAMES)
        country = rnd.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
        phone = f"{PHONE_CODES[country]}{rnd.randint(10 ** 9, 10 ** 10 - 1)}"
        email = f"{first.lower()}.{last.lower()}{person_id}@{rnd.choice(EMAIL_DOMAINS)}"
        birthday = random_date(rnd, BIRTH_START, BIRTH_END)
        people.append((person_id, f"{first} {last}", birthday.isoformat(), country, phone, email))
    return people


def generate_groups(rnd: random.Random, n_groups: int,
                    writer_ids: list[int]) -> tuple[list[tuple], dict[int, list[int]]]:
    groups = []
    members = {}
    max_others = min(MAX_GROUP_MEMBERS, len(writer_ids) - 1)
    for group_id in range(1, n_groups + 1):
        title = f"{rnd.choice(GROUP_TOPICS)} {rnd.choice(GROUP_SUFFIXES)}"
        status = rnd.choices(GROUP_STATUSES, weights=GROUP_STATUS_WEIGHTS)[0]
        created = random_date(rnd, PERIOD_START - timedelta(days=365), PERIOD_START + timedelta(days=30))
        owner_id = rnd.choice(writer_ids)
        others = rnd.sample([p for p in writer_ids if p != owner_id], rnd.randint(min(2, max_others), max_others))
        members[group_id] = [owner_id, *others]
        groups.append((group_id, title, status, created.isoformat(), owner_id))
    return groups, members


def generate_messages(rnd: random.Random, n_messages: int, writer_ids: list[int], all_ids: list[int],
                      groups: list[tuple], members: dict[int, list[int]]) -> list[tuple]:
    group_ids = list(members)
    dead_groups = set(rnd.sample(group_ids, at_least_one(len(group_ids), DEAD_GROUP_SHARE)))
    dead_until = PERIOD_START + timedelta(days=5)
    group_created = {g[0]: date.fromisoformat(g[3]) for g in groups}
    # Bigger groups are more talkative.
    group_weights = [len(members[g]) for g in group_ids]

    def group_message(group_id: int) -> tuple:
        start = max(PERIOD_START, group_created[group_id])
        end = dead_until if group_id in dead_groups else PERIOD_END
        return random_message_time(rnd, start, max(start, end)), rnd.choice(members[group_id]), "", group_id

    def private_message() -> tuple:
        msg_from = rnd.choice(writer_ids)
        msg_to = rnd.choice([p for p in all_ids if p != msg_from])
        return random_message_time(rnd, PERIOD_START, PERIOD_END), msg_from, msg_to, ""

    # One message per group first, so that on a small data set every group (the dead one too) has a last message date.
    headers = [group_message(g) for g in group_ids[:n_messages]]
    while len(headers) < n_messages:
        if rnd.random() < GROUP_MESSAGE_SHARE:
            headers.append(group_message(rnd.choices(group_ids, weights=group_weights)[0]))
        else:
            headers.append(private_message())

    messages = []
    for header in headers:
        status = rnd.choices(MESSAGE_STATUSES, weights=MESSAGE_STATUS_WEIGHTS)[0]
        messages.append((*header, rnd.choice(MESSAGE_TEXTS), status))

    messages.sort(key=lambda m: m[0])
    return [(msg_id, m[0].isoformat(sep=" "), *m[1:]) for msg_id, m in enumerate(messages, start=1)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--people", type=int, default=DEFAULT_PEOPLE)
    parser.add_argument("--groups", type=int, default=DEFAULT_GROUPS)
    parser.add_argument("--messages", type=int, default=DEFAULT_MESSAGES)
    args = parser.parse_args()
    if args.people < 4 or args.groups < 1 or args.messages < 1:
        parser.error("need at least 4 people, 1 group and 1 message")
    return args


def main() -> None:
    args = parse_args()
    rnd = random.Random(SEED)
    people = generate_people(rnd, args.people)
    all_ids = [p[0] for p in people]
    silent_ids = set(rnd.sample(all_ids, at_least_one(args.people, SILENT_PEOPLE_SHARE)))
    writer_ids = [p for p in all_ids if p not in silent_ids]
    groups, members = generate_groups(rnd, args.groups, writer_ids)
    messages = generate_messages(rnd, args.messages, writer_ids, all_ids, groups, members)

    write_csv(OUT_DIR / "people.csv",
              ["person_id", "name", "date_of_birthday", "country", "phone", "email"], people)
    write_csv(OUT_DIR / "groups.csv",
              ["group_id", "title", "status", "created_date", "owner_id"], groups)
    write_csv(OUT_DIR / "messages.csv",
              ["msg_id", "msg_time", "msg_from", "msg_to", "group_id", "text_msg", "status"], messages)


if __name__ == "__main__":
    main()

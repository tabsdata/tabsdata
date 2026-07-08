#!/usr/bin/env python3
"""
Data generator — Airport DB (Flughafen DB).

Generates a self-consistent airport ecosystem: airports + geo, airlines
based at those airports, airplane fleets, employees, passengers,
recurring flight schedules, materialized flights, seat bookings, and
weather observations.

Tables:
    ap_airport ap_airport_geo ap_airline ap_airplane_type ap_airplane
    ap_employee ap_passenger ap_passenger_details
    ap_flight_schedule ap_flight ap_booking ap_weather

Output is one file per table in --out (sql, csv, json or parquet).
"""

import argparse
import random
import string
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path

from tabsdatax.itest.tdksystems.emit import Column, Table, write_tables  # noqa: E402

BASE_SEED = 42

# ---------- Pools ----------
COUNTRIES_CITIES = [
    (
        "United States",
        "New York",
        "Chicago",
        "Los Angeles",
        "Dallas",
        "Atlanta",
        "Denver",
    ),
    ("United Kingdom", "London", "Manchester", "Edinburgh"),
    ("Germany", "Frankfurt", "Munich", "Berlin", "Hamburg"),
    ("France", "Paris", "Lyon", "Nice"),
    ("Spain", "Madrid", "Barcelona", "Valencia"),
    ("Italy", "Rome", "Milan", "Venice"),
    ("Netherlands", "Amsterdam", "Rotterdam"),
    ("Switzerland", "Zurich", "Geneva"),
    ("Austria", "Vienna", "Salzburg"),
    ("Japan", "Tokyo", "Osaka", "Sapporo"),
    ("China", "Beijing", "Shanghai", "Guangzhou"),
    ("India", "Delhi", "Mumbai", "Bangalore"),
    ("Brazil", "Sao Paulo", "Rio de Janeiro"),
    ("Argentina", "Buenos Aires", "Cordoba"),
    ("Australia", "Sydney", "Melbourne"),
    ("Canada", "Toronto", "Vancouver", "Montreal"),
    ("Mexico", "Mexico City", "Guadalajara"),
    ("Russia", "Moscow", "St. Petersburg"),
    ("Turkey", "Istanbul", "Ankara"),
    ("UAE", "Dubai", "Abu Dhabi"),
]

AIRLINE_NAMES = [
    ("LH", "Lufthansa"),
    ("BA", "British Airways"),
    ("AF", "Air France"),
    ("KL", "KLM"),
    ("IB", "Iberia"),
    ("AZ", "ITA Airways"),
    ("OS", "Austrian Airlines"),
    ("LX", "Swiss"),
    ("SK", "SAS"),
    ("AY", "Finnair"),
    ("TK", "Turkish Airlines"),
    ("EK", "Emirates"),
    ("QR", "Qatar Airways"),
    ("EY", "Etihad"),
    ("SQ", "Singapore Airlines"),
    ("CX", "Cathay Pacific"),
    ("JL", "Japan Airlines"),
    ("NH", "ANA"),
    ("KE", "Korean Air"),
    ("OZ", "Asiana"),
    ("DL", "Delta"),
    ("AA", "American Airlines"),
    ("UA", "United"),
    ("WN", "Southwest"),
    ("B6", "JetBlue"),
    ("AS", "Alaska Airlines"),
    ("AC", "Air Canada"),
    ("WS", "WestJet"),
    ("AM", "Aeromexico"),
    ("LA", "LATAM"),
    ("AR", "Aerolineas Argentinas"),
    ("CM", "Copa"),
    ("VS", "Virgin Atlantic"),
    ("EI", "Aer Lingus"),
    ("FR", "Ryanair"),
    ("U2", "easyJet"),
    ("VY", "Vueling"),
    ("W6", "Wizz Air"),
    ("PC", "Pegasus"),
    ("MS", "EgyptAir"),
    ("ET", "Ethiopian Airlines"),
    ("SA", "South African Airways"),
    ("QF", "Qantas"),
    ("NZ", "Air New Zealand"),
]

AIRPLANE_TYPES = [
    ("A319", "Airbus A319-100, narrow-body twin-engine jet"),
    ("A320", "Airbus A320-200, narrow-body twin-engine jet"),
    ("A321", "Airbus A321-200, stretched A320 narrow-body"),
    ("A330", "Airbus A330-300, wide-body twin-engine jet"),
    ("A350", "Airbus A350-900, wide-body composite jet"),
    ("A380", "Airbus A380-800, double-deck wide-body"),
    ("B737", "Boeing 737-800, narrow-body twin-engine jet"),
    ("B738", "Boeing 737 MAX 8, latest 737 variant"),
    ("B739", "Boeing 737 MAX 9, stretched MAX variant"),
    ("B744", "Boeing 747-400, four-engine wide-body"),
    ("B748", "Boeing 747-8, modernised 747 variant"),
    ("B752", "Boeing 757-200, mid-size narrow-body"),
    ("B763", "Boeing 767-300, mid-size wide-body"),
    ("B772", "Boeing 777-200, long-range wide-body"),
    ("B77W", "Boeing 777-300ER, extended-range wide-body"),
    ("B788", "Boeing 787-8 Dreamliner, composite wide-body"),
    ("B789", "Boeing 787-9 Dreamliner, stretched 787"),
    ("E190", "Embraer E190, regional jet"),
    ("E195", "Embraer E195, stretched E190"),
    ("CRJ9", "Bombardier CRJ-900, regional jet"),
    ("AT72", "ATR 72, twin-engine turboprop"),
    ("DH8D", "Bombardier Dash 8 Q400, regional turboprop"),
]

CAPACITY_BY_TYPE = {
    "A319": 144,
    "A320": 180,
    "A321": 220,
    "A330": 290,
    "A350": 325,
    "A380": 525,
    "B737": 162,
    "B738": 175,
    "B739": 178,
    "B744": 416,
    "B748": 467,
    "B752": 200,
    "B763": 269,
    "B772": 314,
    "B77W": 396,
    "B788": 242,
    "B789": 290,
    "E190": 100,
    "E195": 132,
    "CRJ9": 90,
    "AT72": 70,
    "DH8D": 78,
}

DEPARTMENTS = ["Marketing", "Buchhaltung", "Management", "Logistik", "Flugfeld"]
WEATHER_KINDS = [
    "Nebel",
    "Regen",
    "Schneefall",
    "Gewitter",
    "Nebel-Regen",
    "Nebel-Schneefall",
    "Regen-Schneefall",
    "Regen-Gewitter",
    "Nebel-Regen-Gewitter",
]

FIRST_NAMES = [
    "Sophie",
    "Emma",
    "Hannah",
    "Mia",
    "Anna",
    "Lara",
    "Lena",
    "Sarah",
    "Mara",
    "Leonie",
    "Maximilian",
    "Paul",
    "Leon",
    "Felix",
    "Niklas",
    "Tim",
    "Jonas",
    "Lukas",
    "Elias",
    "Liam",
    "Olivia",
    "Ava",
    "Isabella",
    "Charlotte",
    "Amelia",
    "Mila",
    "Aria",
    "Layla",
    "Emily",
    "Harper",
    "Lucas",
    "Mateo",
    "Ethan",
    "Daniel",
    "Carlos",
    "Marco",
    "Diego",
    "Ravi",
    "Aarav",
    "Ahmed",
    "Yuki",
    "Hiro",
    "Mei",
    "Ling",
    "Aiko",
    "Wei",
    "Chen",
    "Priya",
    "Asha",
    "Fatima",
]
LAST_NAMES = [
    "Müller",
    "Schmidt",
    "Schneider",
    "Fischer",
    "Weber",
    "Meyer",
    "Wagner",
    "Becker",
    "Schulz",
    "Hoffmann",
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Davis",
    "Miller",
    "Wilson",
    "Moore",
    "Taylor",
    "Garcia",
    "Rodriguez",
    "Martinez",
    "Lopez",
    "Gonzalez",
    "Perez",
    "Sanchez",
    "Ramirez",
    "Torres",
    "Flores",
    "Rossi",
    "Russo",
    "Ferrari",
    "Esposito",
    "Bianchi",
    "Romano",
    "Ricci",
    "Conti",
    "Costa",
    "Greco",
    "Sato",
    "Suzuki",
    "Takahashi",
    "Tanaka",
    "Watanabe",
    "Ito",
    "Nakamura",
    "Kobayashi",
    "Yamamoto",
    "Kato",
    "Wang",
    "Li",
    "Zhang",
    "Liu",
    "Chen",
    "Yang",
    "Zhao",
    "Huang",
    "Zhou",
    "Wu",
]
STREETS = [
    "Hauptstr.",
    "Bahnhofstr.",
    "Schillerstr.",
    "Goetheweg",
    "Mozartgasse",
    "Lindenallee",
    "Marktplatz",
    "Kirchstr.",
    "Main St",
    "Park Ave",
    "Oak Rd",
    "Elm St",
    "Maple Dr",
]


# ---------- helpers ----------


def rand_iata(used, length):
    while True:
        c = "".join(random.choices(string.ascii_uppercase, k=length))
        if c not in used:
            used.add(c)
            return c


def rand_icao(used):
    while True:
        c = "".join(random.choices(string.ascii_uppercase, k=4))
        if c not in used:
            used.add(c)
            return c


def rand_phone():
    return (
        f"+{random.randint(1, 99)} {random.randint(10, 999)} "
        f"{random.randint(100000, 9999999)}"
    )


def rand_zip():
    return random.randint(10000, 99999)


def rand_md5():
    return "".join(random.choices("0123456789abcdef", k=32))


def rand_passport():
    return random.choice(string.ascii_uppercase) + "".join(
        random.choices(string.digits, k=8)
    )


# ---------- Generators ----------


def gen_airports(n):
    rows = []
    used_iata, used_icao = set(), set()
    flat = [(c, country) for country, *cities in COUNTRIES_CITIES for c in cities]
    for i in range(n):
        city, country = flat[i % len(flat)]
        suffix = "" if i < len(flat) else f" #{i // len(flat) + 1}"
        rows.append({
            "airport_id": i + 1,
            "iata": rand_iata(used_iata, 3),
            "icao": rand_icao(used_icao),
            "name": f"{city}{suffix} International Airport"[:80],
            "city": city,
            "country": country,
            "latitude": round(random.uniform(-60, 70), 6),
            "longitude": round(random.uniform(-180, 180), 6),
        })
    return rows


def gen_airlines(n, airports):
    rows, used = [], set()
    pool = list(AIRLINE_NAMES)
    random.shuffle(pool)
    for i in range(n):
        if i < len(pool):
            iata, name = pool[i]
        else:
            iata = rand_iata(used, 2)
            name = f"Air {iata}"
        used.add(iata)
        base = random.choice(airports[: max(1, len(airports) // 3)])
        rows.append({
            "airline_id": i + 1,
            "iata": iata,
            "airline_name": name,
            "base_airport": base["airport_id"],
        })
    return rows


def gen_airplane_types(n):
    rows, pool = [], list(AIRPLANE_TYPES)
    for i in range(n):
        if i < len(pool):
            ident, desc = pool[i]
        else:
            ident, desc = pool[i % len(pool)]
            ident = f"{ident}-V{i // len(pool) + 1}"
        rows.append({"type_id": i + 1, "identifier": ident, "description": desc})
    return rows


def gen_airplanes(n, types, airlines):
    rows = []
    for i in range(n):
        t = random.choice(types)
        a = random.choice(airlines)
        cap_base = CAPACITY_BY_TYPE.get(t["identifier"].split("-")[0], 180)
        rows.append({
            "airplane_id": i + 1,
            "capacity": cap_base + random.randint(-10, 10),
            "type_id": t["type_id"],
            "airline_id": a["airline_id"],
        })
    return rows


def gen_employees(n):
    rows, used = [], set()
    for i in range(n):
        sex = random.choice(["m", "f"])
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        username = f"{first[0].lower()}{last.lower()}{i + 1}"[:20]
        if username in used:
            username = f"{username}{i}"[:20]
        used.add(username)
        country, *cities = random.choice(COUNTRIES_CITIES)
        rows.append({
            "employee_id": i + 1,
            "firstname": first,
            "lastname": last,
            "birthdate": date(1965, 1, 1) + timedelta(days=random.randint(0, 365 * 35)),
            "sex": sex,
            "street": f"{random.randint(1, 200)} {random.choice(STREETS)}",
            "city": random.choice(cities),
            "zip": rand_zip(),
            "country": country,
            "email": f"{username}@airline.com",
            "phone": rand_phone(),
            "salary": round(random.uniform(28000, 95000), 2),
            "department": random.choice(DEPARTMENTS),
            "username": username,
            "password": rand_md5(),
        })
    return rows


def gen_passengers(n):
    pas, det = [], []
    used = set()
    for i in range(n):
        sex = random.choice(["m", "f"])
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        country, *cities = random.choice(COUNTRIES_CITIES)
        passport = rand_passport()
        while passport in used:
            passport = rand_passport()
        used.add(passport)
        pid = i + 1
        pas.append({
            "passenger_id": pid,
            "passportno": passport,
            "firstname": first,
            "lastname": last,
        })
        det.append({
            "passenger_id": pid,
            "birthdate": date(1940, 1, 1) + timedelta(days=random.randint(0, 365 * 78)),
            "sex": sex,
            "street": f"{random.randint(1, 200)} {random.choice(STREETS)}",
            "city": random.choice(cities),
            "zip": rand_zip(),
            "country": country,
            "email": f"{first.lower()}.{last.lower()}{pid}@example.com",
            "phone": rand_phone(),
        })
    return pas, det


def gen_flight_schedules(n, airports, airlines):
    rows, used = [], set()
    for _ in range(n):
        airline = random.choice(airlines)
        while True:
            flightno = f"{airline['iata']}{random.randint(100, 9999):04d}"[:8]
            if flightno not in used:
                used.add(flightno)
                break
        a = random.choice(airports)
        b = random.choice([x for x in airports if x["airport_id"] != a["airport_id"]])
        dep_h = random.randint(0, 22)
        dep_m = random.choice([0, 15, 30, 45])
        flight_minutes = random.choice([55, 75, 90, 120, 180, 240, 360, 480, 600])
        dep = time(dep_h, dep_m)
        arr_dt = datetime.combine(date(2020, 1, 1), dep) + timedelta(
            minutes=flight_minutes
        )
        arr = arr_dt.time()
        days = [random.random() < 0.55 for _ in range(7)]
        if not any(days):
            days[random.randint(0, 6)] = True
        rows.append({
            "flightno": flightno,
            "from_airport": a["airport_id"],
            "to_airport": b["airport_id"],
            "departure": dep,
            "arrival": arr,
            "airline_id": airline["airline_id"],
            "monday": int(days[0]),
            "tuesday": int(days[1]),
            "wednesday": int(days[2]),
            "thursday": int(days[3]),
            "friday": int(days[4]),
            "saturday": int(days[5]),
            "sunday": int(days[6]),
            "_duration": flight_minutes,
            "_days": days,
        })
    return rows


def gen_flights(n, schedules, airplanes):
    rows = []
    today = date(2024, 12, 31)
    for i in range(n):
        s = random.choice(schedules)
        for _ in range(8):
            day = today - timedelta(days=random.randint(0, 365))
            if s["_days"][day.weekday()]:
                break
        dep = datetime.combine(day, s["departure"])
        arr = dep + timedelta(minutes=s["_duration"])
        same = [p for p in airplanes if p["airline_id"] == s["airline_id"]]
        plane = random.choice(same) if same else random.choice(airplanes)
        rows.append({
            "flight_id": i + 1,
            "flightno": s["flightno"],
            "from_airport": s["from_airport"],
            "to_airport": s["to_airport"],
            "departure": dep,
            "arrival": arr,
            "airline_id": s["airline_id"],
            "airplane_id": plane["airplane_id"],
            "_capacity": plane["capacity"],
        })
    return rows


def gen_bookings(n, flights, passengers):
    rows = []
    if not flights:
        return rows
    per = max(1, n // len(flights))
    target = [per] * len(flights)
    extras = n - per * len(flights)
    for _ in range(max(0, extras)):
        target[random.randrange(len(flights))] += 1
    bid = 1
    for f, count in zip(flights, target):
        cap = f["_capacity"]
        count = min(count, cap)
        used = set()
        for _ in range(count):
            for _ in range(20):
                row = random.randint(1, max(2, cap // 6))
                letter = random.choice("ABCDEF")
                seat = f"{row:02d}{letter}"
                if seat not in used:
                    used.add(seat)
                    break
            p = random.choice(passengers)
            rows.append({
                "booking_id": bid,
                "flight_id": f["flight_id"],
                "seat": seat,
                "passenger_id": p["passenger_id"],
                "price": round(random.uniform(45.0, 1500.0), 2),
            })
            bid += 1
    return rows


def gen_weather(n_stations, n_days):
    rows = []
    today = date(2024, 12, 31)
    for d_off in range(n_days):
        day = today - timedelta(days=d_off)
        for s in range(1, n_stations + 1):
            for h in range(0, 24, 3):
                rows.append({
                    "log_date": day,
                    "log_time": time(h, 0),
                    "station": s,
                    "temp": round(random.uniform(-15, 35), 1),
                    "humidity": round(random.uniform(20, 100), 1),
                    "airpressure": round(random.uniform(950, 1050), 2),
                    "wind": round(random.uniform(0, 40), 2),
                    "weather": random.choice(WEATHER_KINDS + [None] * 4),
                    "winddirection": random.randint(0, 359),
                })
    return rows


# Column specs ---------------------------------------------------------------

AIRPORT_COLS = [
    Column("airport_id", "int", nullable=False, auto=True),
    Column("iata", "char(3)"),
    Column("icao", "char(4)", nullable=False),
    Column("name", "varchar(80)", nullable=False),
]
GEO_COLS = [
    Column("airport_id", "int", nullable=False),
    Column("name", "varchar(80)", nullable=False),
    Column("city", "varchar(80)"),
    Column("country", "varchar(80)"),
    Column("latitude", "decimal(11,8)", nullable=False),
    Column("longitude", "decimal(11,8)", nullable=False),
]
AIRLINE_COLS = [
    Column("airline_id", "int", nullable=False, auto=True),
    Column("iata", "char(2)", nullable=False),
    Column("airline_name", "varchar(50)"),
    Column("base_airport", "int", nullable=False),
]
APTYPE_COLS = [
    Column("type_id", "int", nullable=False, auto=True),
    Column("identifier", "varchar(50)"),
    Column("description", "text"),
]
AIRPLANE_COLS = [
    Column("airplane_id", "int", nullable=False, auto=True),
    Column("capacity", "int", nullable=False),
    Column("type_id", "int", nullable=False),
    Column("airline_id", "int", nullable=False),
]
EMPLOYEE_COLS = [
    Column("employee_id", "int", nullable=False, auto=True),
    Column("firstname", "varchar(100)", nullable=False),
    Column("lastname", "varchar(100)", nullable=False),
    Column("birthdate", "date", nullable=False),
    Column("sex", "char(1)"),
    Column("street", "varchar(100)", nullable=False),
    Column("city", "varchar(100)", nullable=False),
    Column("zip", "int", nullable=False),
    Column("country", "varchar(100)", nullable=False),
    Column("email", "varchar(120)"),
    Column("phone", "varchar(30)"),
    Column("salary", "decimal(8,2)"),
    Column("department", "varchar(20)"),
    Column("username", "varchar(20)"),
    Column("password", "char(32)"),
]
PASSENGER_COLS = [
    Column("passenger_id", "int", nullable=False, auto=True),
    Column("passportno", "char(9)", nullable=False),
    Column("firstname", "varchar(100)", nullable=False),
    Column("lastname", "varchar(100)", nullable=False),
]
PASSDET_COLS = [
    Column("passenger_id", "int", nullable=False),
    Column("birthdate", "date", nullable=False),
    Column("sex", "char(1)"),
    Column("street", "varchar(100)", nullable=False),
    Column("city", "varchar(100)", nullable=False),
    Column("zip", "int", nullable=False),
    Column("country", "varchar(100)", nullable=False),
    Column("email", "varchar(120)"),
    Column("phone", "varchar(30)"),
]
SCHEDULE_COLS = [
    Column("flightno", "char(8)", nullable=False),
    Column("from_airport", "int", nullable=False),
    Column("to_airport", "int", nullable=False),
    Column("departure", "time", nullable=False),
    Column("arrival", "time", nullable=False),
    Column("airline_id", "int", nullable=False),
    Column("monday", "smallint"),
    Column("tuesday", "smallint"),
    Column("wednesday", "smallint"),
    Column("thursday", "smallint"),
    Column("friday", "smallint"),
    Column("saturday", "smallint"),
    Column("sunday", "smallint"),
]
FLIGHT_COLS = [
    Column("flight_id", "int", nullable=False, auto=True),
    Column("flightno", "char(8)", nullable=False),
    Column("from_airport", "int", nullable=False),
    Column("to_airport", "int", nullable=False),
    Column("departure", "datetime", nullable=False),
    Column("arrival", "datetime", nullable=False),
    Column("airline_id", "int", nullable=False),
    Column("airplane_id", "int", nullable=False),
]
BOOKING_COLS = [
    Column("booking_id", "int", nullable=False, auto=True),
    Column("flight_id", "int", nullable=False),
    Column("seat", "char(4)"),
    Column("passenger_id", "int", nullable=False),
    Column("price", "decimal(10,2)", nullable=False),
]
WEATHER_COLS = [
    Column("log_date", "date", nullable=False),
    Column("log_time", "time", nullable=False),
    Column("station", "int", nullable=False),
    Column("temp", "decimal(4,1)", nullable=False),
    Column("humidity", "decimal(4,1)", nullable=False),
    Column("airpressure", "decimal(10,2)", nullable=False),
    Column("wind", "decimal(5,2)", nullable=False),
    Column("weather", "varchar(30)"),
    Column("winddirection", "smallint", nullable=False),
]


def build_tables(args):
    random.seed(args.seed)
    print("Generating airport DB data...", file=sys.stderr)

    airports = gen_airports(args.airports)
    airlines = gen_airlines(args.airlines, airports)
    types = gen_airplane_types(args.airplane_types)
    planes = gen_airplanes(args.airplanes, types, airlines)
    employees = gen_employees(args.employees)
    passengers, pdetails = gen_passengers(args.passengers)
    schedules = gen_flight_schedules(args.flights, airports, airlines)
    flights = gen_flights(args.flights, schedules, planes)
    bookings = gen_bookings(args.bookings, flights, passengers)
    weather = gen_weather(max(1, len(airports) // 5), args.weather_days)

    # Strip helper-only fields
    schedule_clean = [
        {k: v for k, v in s.items() if not k.startswith("_")} for s in schedules
    ]
    flight_clean = [
        {k: v for k, v in f.items() if not k.startswith("_")} for f in flights
    ]

    # Geo rows reuse airport row data
    geo_rows = [
        {
            "airport_id": a["airport_id"],
            "name": a["name"],
            "city": a["city"],
            "country": a["country"],
            "latitude": a["latitude"],
            "longitude": a["longitude"],
        }
        for a in airports
    ]
    airport_rows = [
        {
            "airport_id": a["airport_id"],
            "iata": a["iata"],
            "icao": a["icao"],
            "name": a["name"],
        }
        for a in airports
    ]

    print(
        f"  airports={len(airports)} airlines={len(airlines)} "
        f"types={len(types)} planes={len(planes)} "
        f"employees={len(employees)} passengers={len(passengers)} "
        f"schedules={len(schedules)} flights={len(flights)} "
        f"bookings={len(bookings)} weather={len(weather)}",
        file=sys.stderr,
    )

    return [
        Table("ap_airport", AIRPORT_COLS, airport_rows),
        Table("ap_airport_geo", GEO_COLS, geo_rows),
        Table("ap_airline", AIRLINE_COLS, airlines),
        Table("ap_airplane_type", APTYPE_COLS, types),
        Table("ap_airplane", AIRPLANE_COLS, planes),
        Table("ap_employee", EMPLOYEE_COLS, employees),
        Table("ap_passenger", PASSENGER_COLS, passengers),
        Table("ap_passenger_details", PASSDET_COLS, pdetails),
        Table("ap_flight_schedule", SCHEDULE_COLS, schedule_clean),
        Table("ap_flight", FLIGHT_COLS, flight_clean),
        Table("ap_booking", BOOKING_COLS, bookings),
        Table("ap_weather", WEATHER_COLS, weather),
    ]


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--bookings", type=int, default=10000)
    p.add_argument("--flights", type=int, default=500)
    p.add_argument("--passengers", type=int, default=500)
    p.add_argument("--airports", type=int, default=50)
    p.add_argument("--airlines", type=int, default=20)
    p.add_argument("--airplanes", type=int, default=100)
    p.add_argument("--airplane-types", type=int, default=20, dest="airplane_types")
    p.add_argument("--employees", type=int, default=20)
    p.add_argument("--weather-days", type=int, default=10, dest="weather_days")
    p.add_argument("--format", choices=("sql", "csv", "json", "parquet"), default="sql")
    p.add_argument("--seed", type=int, default=BASE_SEED)
    p.add_argument("--out", required=True)
    p.add_argument("--chunks", type=int, default=None)
    return p.parse_args()


def main():
    args = parse_args()
    tables = build_tables(args)
    write_tables(
        tables,
        args.out,
        args.format,
        source_dir=Path(__file__).parent,
        chunks=args.chunks,
    )


if __name__ == "__main__":
    main()

#
# Copyright 2026 Tabsdata Inc.
#

"""Data generator for the ``courierlogistics`` model.

A synthetic parcel-delivery dataset — distribution hubs, couriers, customers,
shipments, and a realistic per-shipment tracking-event chain (pickup, hub
transfers, out-for-delivery, and a delivered / failed / returned / in-transit
outcome). The requested size is the shipment count; couriers, customers, and
hubs scale from it. Reference-only: there is no delta recipe, so ``create-data``
works but ``modify-data`` has nothing to change.
"""

from __future__ import annotations

import random
from collections import defaultdict
from datetime import datetime, timedelta

from tabsdatax.itest.tdksystems2.datagen import Entity, Model
from tabsdatax.itest.tdksystems2.emit import Column, Table

BASE_SEED = 42

# Scaling (from the v1 sizing.json): shipments is the prolific flag, and the
# other counts scale from it against a base of 1000 shipments.
_PROLIFIC_BASE = 1000
_DEFAULTS = {"customers": 200, "couriers": 50, "hubs": 10}
_MINS = {"hubs": 2}

# ---------------------------------------------------------------------------
# Pools
# ---------------------------------------------------------------------------
HUB_DATA = [
    # (code, city, country, street)
    ("MAD-01", "Madrid", "ES", "Calle Industria 40, Vallecas"),
    ("BCN-01", "Barcelona", "ES", "Carrer de la Logística 12, Zona Franca"),
    ("VLC-01", "Valencia", "ES", "Polígono Fuente del Jarro, Nave 8"),
    ("SEV-01", "Seville", "ES", "Pol. Ind. Carretera Amarilla s/n"),
    ("BIL-01", "Bilbao", "ES", "Muelle de Axpe 14"),
    ("ZGZ-01", "Zaragoza", "ES", "Polígono Cogullada, Nave 22"),
    ("MAL-01", "Malaga", "ES", "Pol. Ind. Santa Bárbara, Nave 5"),
    ("VLL-01", "Valladolid", "ES", "Carretera de Salamanca km 3"),
    ("ALC-01", "Alicante", "ES", "Pol. Ind. Pla de la Vallonga, Nave 9"),
    ("MUR-01", "Murcia", "ES", "Pol. Ind. El Palmar, Calle B"),
    ("LIS-01", "Lisbon", "PT", "Rua da Logística 20, Sacavém"),
    ("OPO-01", "Porto", "PT", "Rua do Freixo 100, Maia"),
    ("PAR-01", "Paris", "FR", "ZAC de la Haute Borne, Villeneuve d'Ascq"),
    ("LYO-01", "Lyon", "FR", "Rue de la Cartoucherie 10, Vénissieux"),
    ("AMS-01", "Amsterdam", "NL", "Schipholweg 55, Badhoevedorp"),
]

VEHICLE_TYPES = ["bicycle", "motorbike", "van", "truck"]
VEH_WEIGHTS = [10, 20, 50, 20]

FIRST_M = [
    "Lucas",
    "Andreu",
    "Roberto",
    "Javier",
    "Miguel",
    "Tomás",
    "Pablo",
    "Carlos",
    "Iñigo",
    "Sérgio",
]
FIRST_F = [
    "Marta",
    "Beatriz",
    "Laura",
    "Ana",
    "Elena",
    "Sofía",
    "Claudia",
    "Isabel",
    "Raquel",
    "Catarina",
]
LAST = [
    "García",
    "Soler",
    "Puig",
    "Lemos",
    "Nunes",
    "Martín",
    "López",
    "Sánchez",
    "Fernández",
    "González",
    "Rodríguez",
    "Pérez",
    "Gómez",
    "Ruiz",
    "Díaz",
    "Alves",
    "Ferreira",
    "Costa",
    "Santos",
    "Oliveira",
]

BUSSINESS_NAMES = [
    "Tecnología Rápida S.L.",
    "ElectroShop Online S.A.",
    "ModaMujer S.L.",
    "Farmacia Digital S.L.",
    "LibrosDireto S.A.",
    "AutoPiezas Express",
    "Joyería Online S.L.",
    "SportGear Pro S.A.",
    "HomeDecor Plus S.L.",
    "BabyShop Online S.A.",
    "Óptica Virtual S.L.",
    "MascotaFeliz S.A.",
    "TechGadgets S.L.",
    "VinoPremium S.A.",
    "FloristeríaNet S.L.",
    "IndustrialParts S.A.",
    "OficinaTotal S.L.",
    "GourmetBox S.A.",
]

SERVICE_TYPES = ["standard", "express", "overnight", "economy"]
STYPE_W = [45, 25, 10, 20]

STATUSES = [
    "picked_up",
    "in_transit",
    "at_hub",
    "out_for_delivery",
    "delivered",
    "failed",
    "returned",
]
# We'll compute status from tracking events instead of random

STREETS = [
    "Calle Mayor",
    "Av. Principal",
    "Paseo Central",
    "Calle Real",
    "Av. del Parque",
    "Calle Nueva",
    "Passeig de Gràcia",
    "Calle Gran Vía",
    "Rambla Principal",
    "Av. de la Constitución",
]

CITIES = ["Madrid", "Barcelona", "Valencia", "Seville", "Bilbao"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def rand_date(start, end):
    delta = end - start
    return start + timedelta(
        hours=random.randint(0, int(delta.total_seconds() // 3600))
    )


def fmt_date(d):
    return d.strftime("%Y-%m-%d")


def fmt_dt(d):
    return d.strftime("%Y-%m-%d %H:%M:%S")


def rand_name(sex="M"):
    first = random.choice(FIRST_M if sex == "M" else FIRST_F)
    return f"{first} {random.choice(LAST)}"


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------


def gen_hubs(n):
    rows = []
    pool = HUB_DATA * ((n // len(HUB_DATA)) + 2)
    for i in range(1, n + 1):
        code, city, country, street = pool[i - 1]
        rows.append({
            "hub_id": i,
            "code": code if i <= len(HUB_DATA) else f"HUB-{i:02d}",
            "name": f"{city} {'Central' if i % 3 == 1 else 'Distribution'} Hub",
            "address": street,
            "city": city,
            "country": country,
        })
    return rows


def gen_couriers(n, hubs):
    rows = []
    for i in range(1, n + 1):
        sex = random.choice(["M", "F"])
        name = rand_name(sex)
        vtype = random.choices(VEHICLE_TYPES, weights=VEH_WEIGHTS)[0]
        plate = None
        if vtype in ("motorbike", "van", "truck"):
            plate = (
                f"{random.choice(['M', 'B', 'V', 'SE'])}-"
                f"{random.randint(1000, 9999)}-"
                f"{random.choice('ABCDEFGHJ')}{random.choice('KLMNPRSTV')}"
            )
        rows.append({
            "courier_id": i,
            "employee_no": f"COU-{i:03d}",
            "name": name,
            "phone": (
                f"+34 6{random.randint(0, 99):02d} {random.randint(100000, 999999)}"
            ),
            "vehicle_type": vtype,
            "vehicle_plate": plate,
            "home_hub_id": random.choice(hubs)["hub_id"],
        })
    return rows


def gen_customers(n):
    rows = []
    for i in range(1, n + 1):
        is_biz = random.random() < 0.35
        if is_biz:
            name = random.choice(BUSSINESS_NAMES)
            email = f"logistica@{name.split()[0].lower()}{i}.es"
        else:
            sex = random.choice(["M", "F"])
            name = rand_name(sex)
            last = name.split()[-1].lower()
            email = f"{last}{i}@gmail.com"
        rows.append({
            "customer_id": i,
            "name": name,
            "phone": (
                f"+34 6{random.randint(11, 99):02d} {random.randint(100000, 999999)}"
            ),
            "email": email,
            "default_address": (
                f"{random.choice(STREETS)} {random.randint(1, 200)}, "
                f"{random.choice(CITIES)}"
            ),
            "account_type": "business" if is_biz else "personal",
        })
    return rows


def gen_shipments(n, customers, hubs):
    rows = []
    start = datetime(2023, 1, 1)
    end = datetime(2024, 11, 30)
    for i in range(1, n + 1):
        sender = random.choice(customers)
        oh = random.choice(hubs)
        dh = random.choice([h for h in hubs if h["hub_id"] != oh["hub_id"]])
        stype = random.choices(SERVICE_TYPES, weights=STYPE_W)[0]
        created = rand_date(start, end)
        if stype == "overnight":
            transit_days = 1
        elif stype == "express":
            transit_days = random.randint(1, 3)
        elif stype == "standard":
            transit_days = random.randint(2, 5)
        else:
            transit_days = random.randint(5, 10)
        est_delivery = created + timedelta(days=transit_days)
        sex = random.choice(["M", "F"])
        rname = rand_name(sex)
        weight = round(random.uniform(0.1, 30.0), 3)
        length, width, height = (
            random.randint(10, 80),
            random.randint(10, 60),
            random.randint(5, 50),
        )
        rows.append({
            "shipment_id": i,
            "tracking_no": f"TRK-{oh['country']}-{created.year}-{i:06d}",
            "sender_id": sender["customer_id"],
            "recipient_name": rname,
            "recipient_phone": (
                f"+34 6{random.randint(11, 99):02d} {random.randint(100000, 999999)}"
            ),
            "recipient_addr": (
                f"{random.choice(STREETS)} {random.randint(1, 200)}, {dh['city']}"
            ),
            "origin_hub_id": oh["hub_id"],
            "dest_hub_id": dh["hub_id"],
            "weight_kg": weight,
            "dimensions_cm": f"{length}x{width}x{height}",
            "service_type": stype,
            "declared_value": round(random.uniform(10, 2000), 2)
            if random.random() < 0.7
            else None,
            "created_at": fmt_dt(created),
            "estimated_delivery": fmt_date(est_delivery),
            "_created_dt": created,  # kept for event generation
            "_transit_days": transit_days,
        })
    return sorted(rows, key=lambda r: r["created_at"])


def gen_tracking_events(shipments, hubs, couriers):
    """Generate a realistic event chain per shipment."""
    rows = []
    eid = 1
    hub_map = {h["hub_id"]: h for h in hubs}

    for shi in shipments:
        created = shi["_created_dt"]
        oh_id = shi["origin_hub_id"]
        dh_id = shi["dest_hub_id"]
        td = shi["_transit_days"]  # noqa: F841
        courier = random.choice(couriers)

        # Roll final outcome
        outcome = random.choices(
            ["delivered", "failed", "returned", "in_transit"], weights=[75, 8, 5, 12]
        )[0]

        t = created

        # 1. Picked up
        rows.append({
            "event_id": eid,
            "shipment_id": shi["shipment_id"],
            "hub_id": oh_id,
            "courier_id": courier["courier_id"],
            "event_type": "picked_up",
            "location_desc": hub_map[oh_id]["name"],
            "notes": None,
            "occurred_at": fmt_dt(t),
        })
        eid += 1
        t += timedelta(hours=random.randint(1, 4))

        # 2. Departed origin hub
        rows.append({
            "event_id": eid,
            "shipment_id": shi["shipment_id"],
            "hub_id": oh_id,
            "courier_id": courier["courier_id"],
            "event_type": "departed_hub",
            "location_desc": hub_map[oh_id]["name"],
            "notes": f"En route to {hub_map[dh_id]['city']}",
            "occurred_at": fmt_dt(t),
        })
        eid += 1
        t += timedelta(hours=random.randint(4, 24))

        # 3. Possibly transit hub
        if outcome != "in_transit" and random.random() < 0.4 and len(hubs) > 2:
            transit_candidates = [h for h in hubs if h["hub_id"] not in (oh_id, dh_id)]
            if transit_candidates:
                th = random.choice(transit_candidates)
                rows.append({
                    "event_id": eid,
                    "shipment_id": shi["shipment_id"],
                    "hub_id": th["hub_id"],
                    "courier_id": None,
                    "event_type": "arrived_hub",
                    "location_desc": th["name"],
                    "notes": None,
                    "occurred_at": fmt_dt(t),
                })
                eid += 1
                t += timedelta(hours=random.randint(2, 8))
                rows.append({
                    "event_id": eid,
                    "shipment_id": shi["shipment_id"],
                    "hub_id": th["hub_id"],
                    "courier_id": None,
                    "event_type": "departed_hub",
                    "location_desc": th["name"],
                    "notes": f"En route to {hub_map[dh_id]['city']}",
                    "occurred_at": fmt_dt(t),
                })
                eid += 1
                t += timedelta(hours=random.randint(4, 18))

        if outcome == "in_transit":
            # Stop here — shipment still in transit
            rows.append({
                "event_id": eid,
                "shipment_id": shi["shipment_id"],
                "hub_id": dh_id,
                "courier_id": None,
                "event_type": "arrived_hub",
                "location_desc": hub_map[dh_id]["name"],
                "notes": None,
                "occurred_at": fmt_dt(t),
            })
            eid += 1
            continue

        # 4. Arrived destination hub
        rows.append({
            "event_id": eid,
            "shipment_id": shi["shipment_id"],
            "hub_id": dh_id,
            "courier_id": None,
            "event_type": "arrived_hub",
            "location_desc": hub_map[dh_id]["name"],
            "notes": None,
            "occurred_at": fmt_dt(t),
        })
        eid += 1
        t += timedelta(hours=random.randint(2, 8))

        # 5. Out for delivery
        del_courier = random.choice(couriers)
        rows.append({
            "event_id": eid,
            "shipment_id": shi["shipment_id"],
            "hub_id": dh_id,
            "courier_id": del_courier["courier_id"],
            "event_type": "out_for_delivery",
            "location_desc": hub_map[dh_id]["name"],
            "notes": None,
            "occurred_at": fmt_dt(t),
        })
        eid += 1
        t += timedelta(hours=random.randint(2, 6))

        if outcome == "delivered":
            recipient = shi["recipient_name"].split()[-1]
            rows.append({
                "event_id": eid,
                "shipment_id": shi["shipment_id"],
                "hub_id": None,
                "courier_id": del_courier["courier_id"],
                "event_type": "delivered",
                "location_desc": shi["recipient_addr"],
                "notes": random.choice([
                    f"Signed by: {recipient}",
                    "Left with neighbour",
                    "Left at front door per instruction",
                    "Received by concierge",
                ]),
                "occurred_at": fmt_dt(t),
            })
            eid += 1

        elif outcome == "failed":
            rows.append({
                "event_id": eid,
                "shipment_id": shi["shipment_id"],
                "hub_id": None,
                "courier_id": del_courier["courier_id"],
                "event_type": "delivery_attempted",
                "location_desc": shi["recipient_addr"],
                "notes": random.choice([
                    "No answer; card left",
                    "Address not found",
                    "Recipient refused delivery",
                ]),
                "occurred_at": fmt_dt(t),
            })
            eid += 1
            t += timedelta(hours=random.randint(24, 48))
            rows.append({
                "event_id": eid,
                "shipment_id": shi["shipment_id"],
                "hub_id": None,
                "courier_id": del_courier["courier_id"],
                "event_type": "exception",
                "location_desc": shi["recipient_addr"],
                "notes": "Second attempt failed; held at hub for collection",
                "occurred_at": fmt_dt(t),
            })
            eid += 1

        elif outcome == "returned":
            rows.append({
                "event_id": eid,
                "shipment_id": shi["shipment_id"],
                "hub_id": dh_id,
                "courier_id": del_courier["courier_id"],
                "event_type": "returned",
                "location_desc": hub_map[dh_id]["name"],
                "notes": "Returned to hub; customer requested return",
                "occurred_at": fmt_dt(t),
            })
            eid += 1

    return rows


# Determine final status from events
def final_status(events_for_shipment):
    if not events_for_shipment:
        return "picked_up"
    last_type = events_for_shipment[-1]["event_type"]
    mapping = {
        "delivered": "delivered",
        "exception": "failed",
        "returned": "returned",
        "out_for_delivery": "out_for_delivery",
        "arrived_hub": "at_hub",
        "departed_hub": "in_transit",
        "picked_up": "picked_up",
        "delivery_attempted": "failed",
    }
    return mapping.get(last_type, "in_transit")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

HUB_COLS = [
    Column("hub_id", "int", nullable=False, auto=True),
    Column("code", "varchar(20)", nullable=False),
    Column("name", "varchar(100)", nullable=False),
    Column("address", "varchar(200)", nullable=False),
    Column("city", "varchar(80)", nullable=False),
    Column("country", "char(2)", nullable=False),
]
COURIER_COLS = [
    Column("courier_id", "int", nullable=False, auto=True),
    Column("employee_no", "varchar(20)", nullable=False),
    Column("name", "varchar(100)", nullable=False),
    Column("phone", "varchar(30)"),
    Column("vehicle_type", "varchar(20)", nullable=False),
    Column("vehicle_plate", "varchar(20)"),
    Column("home_hub_id", "int", nullable=False),
]
CUSTOMER_COLS = [
    Column("customer_id", "int", nullable=False, auto=True),
    Column("name", "varchar(150)", nullable=False),
    Column("phone", "varchar(30)"),
    Column("email", "varchar(255)"),
    Column("default_address", "varchar(300)"),
    Column("account_type", "varchar(20)", nullable=False),
]
SHIPMENT_COLS = [
    Column("shipment_id", "int", nullable=False, auto=True),
    Column("tracking_no", "varchar(50)", nullable=False),
    Column("sender_id", "int", nullable=False),
    Column("recipient_name", "varchar(150)", nullable=False),
    Column("recipient_phone", "varchar(30)"),
    Column("recipient_addr", "varchar(300)", nullable=False),
    Column("origin_hub_id", "int", nullable=False),
    Column("dest_hub_id", "int", nullable=False),
    Column("weight_kg", "decimal(8,3)", nullable=False),
    Column("dimensions_cm", "varchar(20)"),
    Column("service_type", "varchar(20)", nullable=False),
    Column("declared_value", "decimal(10,2)"),
    Column("status", "varchar(20)", nullable=False),
    Column("created_at", "datetime", nullable=False),
    Column("estimated_delivery", "date"),
]
EVENT_COLS = [
    Column("event_id", "int", nullable=False, auto=True),
    Column("shipment_id", "int", nullable=False),
    Column("hub_id", "int"),
    Column("courier_id", "int"),
    Column("event_type", "varchar(30)", nullable=False),
    Column("location_desc", "varchar(200)"),
    Column("notes", "varchar(300)"),
    Column("occurred_at", "datetime", nullable=False),
]


def _scaled(flag: str, n: int) -> int:
    return max(_DEFAULTS[flag] * n // _PROLIFIC_BASE, _MINS.get(flag, 1))


class _CourierLogistics(Model):
    entities: list[Entity] = []  # reference-only: no deltas

    def build_initial(self, rng: random.Random, n: int) -> list[Table]:
        random.seed(BASE_SEED)

        hubs = gen_hubs(_scaled("hubs", n))
        couriers = gen_couriers(_scaled("couriers", n), hubs)
        customers = gen_customers(_scaled("customers", n))
        shipments = gen_shipments(n, customers, hubs)
        tracking_events = gen_tracking_events(shipments, hubs, couriers)

        events_by_ship = defaultdict(list)
        for ev in tracking_events:
            events_by_ship[ev["shipment_id"]].append(ev)
        for s in shipments:
            s["status"] = final_status(events_by_ship[s["shipment_id"]])
            s.pop("_created_dt", None)
            s.pop("_transit_days", None)

        return [
            Table("hubs", HUB_COLS, hubs),
            Table("couriers", COURIER_COLS, couriers),
            Table("customers", CUSTOMER_COLS, customers),
            Table("shipments", SHIPMENT_COLS, shipments),
            Table("tracking_events", EVENT_COLS, tracking_events),
        ]


def datagenerator() -> Model:
    return _CourierLogistics()

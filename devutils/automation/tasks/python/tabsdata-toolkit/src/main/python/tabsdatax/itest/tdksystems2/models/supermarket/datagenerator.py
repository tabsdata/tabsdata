#
# Copyright 2026 Tabsdata Inc.
#

"""Data generator for the ``supermarket`` model.

Builds a deterministic grocery inventory dataset — suppliers, a category tree,
products, warehouses, per-warehouse stock levels and stock movements — scaled by
the requested size (the *movements* count = the size bucket; suppliers, products
and warehouses scale from it). Reference-only: there is no delta recipe, so
``create-data`` works but ``modify-data`` has no entities to change.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from tabsdatax.itest.tdksystems2.datagen import Entity, Model
from tabsdatax.itest.tdksystems2.emit import Column, Table

# The *movements* count is the size bucket; suppliers / products / warehouses
# scale from it, reproducing v1 supermarket/sizing.json (prolific=--movements,
# base=1000).
_PROLIFIC_BASE = 1000
_DEFAULTS = {
    "suppliers": 10,
    "products": 200,
    "warehouses": 5,
}
_MINS: dict[str, int] = {}
_CAPS: dict[str, int] = {}


def _sized(flag: str, n: int) -> int:
    v = max(_DEFAULTS[flag] * n // _PROLIFIC_BASE, _MINS.get(flag, 1))
    if flag in _CAPS:
        v = min(v, _CAPS[flag])
    return v


# ---------- Pools ----------
SUPPLIER_NAMES = [
    "FreshFarm Produce",
    "Green Valley Foods",
    "Iberian Dairy Co.",
    "Nordic Meats",
    "Global Beverages",
    "MegaPack Dry Goods",
    "SunGrow Organics",
    "Alpine Dairy",
    "Pacific Seafood",
    "Golden Grain Mills",
    "Tropical Imports",
    "Local Harvest Co.",
    "Urban Bakeries",
    "Summit Snacks",
    "Heritage Farms",
    "BlueSea Fish Co.",
    "Sunrise Eggs Ltd.",
    "Cascade Beverages",
    "Prime Cut Meats",
    "NaturePure Oils",
]
CONTACT_FIRST = [
    "Carlos",
    "Ana",
    "Piet",
    "Sofia",
    "Maria",
    "James",
    "Laura",
    "David",
    "Yuki",
    "Elena",
    "Marco",
    "Rachel",
    "Tom",
    "Priya",
]
CONTACT_LAST = [
    "Mendez",
    "Ferreira",
    "van Dijk",
    "Rossi",
    "Santos",
    "Okafor",
    "Walsh",
    "Osei",
    "Tanaka",
    "Brandt",
    "Bianchi",
    "Hughes",
    "Bergmann",
    "Nair",
]
CITIES = [
    "Madrid",
    "Barcelona",
    "Valencia",
    "Seville",
    "Bilbao",
    "Milan",
    "Amsterdam",
    "Paris",
    "London",
    "Lisbon",
    "Berlin",
    "Rome",
]

CATEGORY_TREE = [
    ("Fresh", None),
    ("Fruit", "Fresh"),
    ("Vegetables", "Fresh"),
    ("Dairy", None),
    ("Milk", "Dairy"),
    ("Cheese", "Dairy"),
    ("Yoghurt", "Dairy"),
    ("Beverages", None),
    ("Water", "Beverages"),
    ("Soft Drinks", "Beverages"),
    ("Juices", "Beverages"),
    ("Beer & Wine", "Beverages"),
    ("Dry Goods", None),
    ("Pasta", "Dry Goods"),
    ("Rice", "Dry Goods"),
    ("Cereals", "Dry Goods"),
    ("Bakery", None),
    ("Bread", "Bakery"),
    ("Pastries", "Bakery"),
    ("Frozen", None),
    ("Frozen Meals", "Frozen"),
    ("Ice Cream", "Frozen"),
    ("Meat & Fish", None),
    ("Meat", "Meat & Fish"),
    ("Fish", "Meat & Fish"),
    ("Snacks", None),
    ("Crisps", "Snacks"),
    ("Chocolate", "Snacks"),
    ("Cleaning", None),
    ("Personal Care", None),
]

PRODUCT_TEMPLATES = [
    # (name_template, category, unit, cost_lo, cost_hi, sell_lo, sell_hi, vat)
    ("Gala Apples", "Fruit", "kg", 0.40, 0.70, 0.79, 1.29, 4.00),
    ("Cavendish Bananas", "Fruit", "kg", 0.30, 0.55, 0.59, 0.99, 4.00),
    ("Strawberries 400g", "Fruit", "pack", 0.80, 1.20, 1.49, 2.49, 4.00),
    ("Seedless Grapes", "Fruit", "kg", 1.00, 1.80, 1.99, 3.29, 4.00),
    ("Cherry Tomatoes 500g", "Vegetables", "pack", 0.55, 0.90, 1.19, 1.99, 4.00),
    ("Iceberg Lettuce", "Vegetables", "unit", 0.30, 0.50, 0.59, 0.99, 4.00),
    ("Broccoli", "Vegetables", "unit", 0.50, 0.90, 0.99, 1.79, 4.00),
    ("Carrots 1kg", "Vegetables", "pack", 0.40, 0.70, 0.79, 1.49, 4.00),
    ("Whole Milk 1L", "Milk", "litre", 0.45, 0.70, 0.89, 1.29, 4.00),
    ("Skimmed Milk 1L", "Milk", "litre", 0.40, 0.65, 0.79, 1.19, 4.00),
    ("Greek Yoghurt 500g", "Yoghurt", "unit", 0.70, 1.10, 1.29, 1.99, 4.00),
    ("Manchego Cheese 200g", "Cheese", "unit", 1.80, 2.80, 3.49, 5.99, 4.00),
    ("Cheddar Slice 150g", "Cheese", "unit", 0.90, 1.50, 1.79, 2.99, 4.00),
    ("Still Water 1.5L", "Water", "unit", 0.12, 0.25, 0.39, 0.69, 21.00),
    ("Sparkling Water 1L", "Water", "unit", 0.14, 0.28, 0.45, 0.79, 21.00),
    ("Cola 2L", "Soft Drinks", "unit", 0.50, 0.80, 1.09, 1.79, 21.00),
    ("Lemon Soda 330ml", "Soft Drinks", "unit", 0.20, 0.40, 0.49, 0.89, 21.00),
    ("Orange Juice 1L", "Juices", "litre", 0.60, 1.00, 1.19, 1.99, 21.00),
    ("Apple Juice 1L", "Juices", "litre", 0.55, 0.90, 1.09, 1.79, 21.00),
    ("Lager Beer 6-pack", "Beer & Wine", "pack", 2.50, 4.00, 4.99, 7.99, 21.00),
    ("Red Wine 75cl", "Beer & Wine", "unit", 2.00, 5.00, 4.49, 9.99, 21.00),
    ("Spaghetti 500g", "Pasta", "unit", 0.25, 0.50, 0.59, 1.09, 21.00),
    ("Penne 500g", "Pasta", "unit", 0.25, 0.50, 0.59, 1.09, 21.00),
    ("Fusilli 500g", "Pasta", "unit", 0.28, 0.55, 0.65, 1.15, 21.00),
    ("Basmati Rice 1kg", "Rice", "kg", 0.55, 0.90, 1.09, 1.99, 21.00),
    ("Arborio Rice 1kg", "Rice", "kg", 0.80, 1.20, 1.59, 2.49, 21.00),
    ("Cornflakes 500g", "Cereals", "unit", 0.60, 1.00, 1.29, 2.19, 21.00),
    ("Muesli 750g", "Cereals", "unit", 1.00, 1.80, 1.99, 3.49, 21.00),
    ("Sourdough Loaf", "Bread", "unit", 0.80, 1.40, 1.49, 2.49, 21.00),
    ("Croissant x4", "Pastries", "pack", 0.90, 1.50, 1.79, 2.99, 21.00),
    ("Chicken Breast 600g", "Meat", "pack", 2.50, 3.80, 4.49, 6.99, 4.00),
    ("Beef Mince 500g", "Meat", "pack", 2.20, 3.50, 3.99, 6.49, 4.00),
    ("Salmon Fillet 2pk", "Fish", "pack", 4.00, 6.50, 6.99, 10.99, 4.00),
    ("Cod Fillets 400g", "Fish", "pack", 2.80, 4.50, 4.99, 7.99, 4.00),
    ("Frozen Pizza Marg.", "Frozen Meals", "unit", 1.20, 2.20, 2.49, 3.99, 21.00),
    ("Vanilla Ice Cream 1L", "Ice Cream", "litre", 1.20, 2.00, 2.29, 3.79, 21.00),
    ("Crisps 150g", "Crisps", "unit", 0.50, 0.90, 0.99, 1.79, 21.00),
    ("Dark Chocolate 100g", "Chocolate", "unit", 0.60, 1.20, 1.19, 2.29, 21.00),
    ("Milk Chocolate 100g", "Chocolate", "unit", 0.50, 1.00, 0.99, 1.99, 21.00),
    ("Dishwasher Tabs 30pk", "Cleaning", "unit", 2.00, 3.50, 3.99, 6.99, 21.00),
    ("Shampoo 400ml", "Personal Care", "unit", 1.00, 2.20, 1.99, 4.49, 21.00),
]

BRANDS = [
    "FreshFarm",
    "NaturePure",
    "GlobalBrand",
    "HomeChoice",
    "PrimeCut",
    "SunGrow",
    "AlpineAire",
    "MegaPack",
    "BlueStar",
    "EcoFresh",
    "HeritageValue",
    "GoldenField",
    "CasaVerde",
    "Urban",
    "Nordic",
]

MOVEMENT_TYPES = ["receipt", "sale", "adjustment", "transfer", "waste"]
MOVEMENT_REFS = [
    "PO-{y}-{n}",
    "POS-{y}{n}",
    "ADJ-{y}-{n}",
    "TRF-{y}-{n}",
    "WASTE-{y}{n}",
]

WAREHOUSE_NAMES = [
    ("Main Store Floor", "Aisle 1-12, Ground Floor"),
    ("Back Stockroom", "Rear Building, Ground Floor"),
    ("Cold Storage", "Refrigerated Unit A"),
    ("Dry Goods Annex", "North Wing, Floor 1"),
    ("Bulk Storage", "External Warehouse, Bay 3"),
    ("Seasonal Store", "Mezzanine Level"),
    ("Produce Room", "Ground Floor, Fresh Zone"),
]


# ---------- helpers ----------


def rand_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ---------- Generators ----------


def gen_suppliers(n: int) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        base = SUPPLIER_NAMES[i % len(SUPPLIER_NAMES)]
        name = base if i <= len(SUPPLIER_NAMES) else f"{base} #{i}"
        first = random.choice(CONTACT_FIRST)
        last = random.choice(CONTACT_LAST)
        city = random.choice(CITIES)
        rows.append({
            "supplier_id": i,
            "name": name,
            "contact_name": f"{first} {last}",
            "phone": (
                f"+{random.randint(30, 49)} {random.randint(10, 99)} "
                f"{random.randint(100, 999)} {random.randint(1000, 9999)}"
            ),
            "email": (
                f"{last.lower().replace(' ', '')}@"
                f"{name.lower().replace(' ', '').replace('.', '')[:12]}.com"
            ),
            "address": (
                f"{random.choice(['Calle', 'Av.', 'Via', 'Rue', 'Street'])} "
                f"{random.choice(['Mayor', 'Roma', 'Grande', 'Norte', 'Central'])} "
                f"{random.randint(1, 200)}, {city}"
            ),
        })
    return rows


def gen_categories() -> list[dict]:
    rows = []
    id_map: dict[str, int] = {}
    for i, (name, parent_name) in enumerate(CATEGORY_TREE, 1):
        parent_id = id_map.get(parent_name) if parent_name is not None else None
        id_map[name] = i
        rows.append({"category_id": i, "name": name, "parent_id": parent_id})
    return rows


def gen_products(n: int, suppliers: list[dict], categories: list[dict]) -> list[dict]:
    cat_map = {c["name"]: c["category_id"] for c in categories}
    leaf_cats = [c for c in categories if c["parent_id"] is not None]
    rows = []
    templates = PRODUCT_TEMPLATES * ((n // len(PRODUCT_TEMPLATES)) + 2)
    random.shuffle(templates)
    for i in range(1, n + 1):
        tpl = templates[i - 1]
        name, cat_name, unit, cl, ch, sl, sh, vat = tpl
        # Add suffix variant to avoid duplicate names
        suffix = f" #{i}" if i > len(PRODUCT_TEMPLATES) else ""
        cat_id = cat_map.get(cat_name, random.choice(leaf_cats)["category_id"])
        # Generate unique SKU
        prefix = cat_name[:3].upper().replace(" ", "")
        sku = f"{prefix}-{i:04d}"
        cost = round(random.uniform(cl, ch), 2)
        sell = round(random.uniform(max(cost * 1.1, sl), sh), 2)
        rows.append({
            "product_id": i,
            "sku": sku,
            "name": name + suffix,
            "brand": random.choice(BRANDS),
            "category_id": cat_id,
            "supplier_id": random.choice(suppliers)["supplier_id"],
            "unit": unit,
            "cost_price": cost,
            "sell_price": sell,
            "vat_rate": vat,
            "is_active": 1 if random.random() < 0.95 else 0,
        })
    return rows


def gen_warehouses(n: int) -> list[dict]:
    rows = []
    pool = WAREHOUSE_NAMES * ((n // len(WAREHOUSE_NAMES)) + 2)
    for i in range(1, n + 1):
        name, loc = pool[i - 1]
        rows.append({"warehouse_id": i, "name": name, "location": loc})
    return rows


def gen_stock(products: list[dict], warehouses: list[dict]) -> list[dict]:
    """Each product gets stock in 1..3 warehouses."""
    rows = []
    sid = 1
    for p in products:
        wh_count = random.randint(1, min(3, len(warehouses)))
        wh_sample = random.sample(warehouses, wh_count)
        for w in wh_sample:
            qty = round(random.uniform(0, 500), 3)
            reorder = round(random.uniform(10, 80), 3)
            rows.append({
                "stock_id": sid,
                "product_id": p["product_id"],
                "warehouse_id": w["warehouse_id"],
                "quantity": qty,
                "reorder_level": reorder,
                "reorder_qty": round(reorder * random.uniform(2, 5), 3),
            })
            sid += 1
    return rows


def gen_movements(n: int, products: list[dict], warehouses: list[dict]) -> list[dict]:
    rows = []
    start = datetime(2024, 1, 1)
    end = datetime(2024, 12, 31)
    for i in range(1, n + 1):
        mtype = random.choice(MOVEMENT_TYPES)
        qty = round(random.uniform(1, 200), 3)
        if mtype in ("sale", "waste"):
            qty = -qty
        dt = rand_date(start, end)
        ref_tpl = random.choice(MOVEMENT_REFS)
        ref = ref_tpl.format(y=dt.year, n=random.randint(1000, 9999))
        rows.append({
            "movement_id": i,
            "product_id": random.choice(products)["product_id"],
            "warehouse_id": random.choice(warehouses)["warehouse_id"],
            "movement_type": mtype,
            "quantity": qty,
            "reference": ref,
            "moved_at": fmt_dt(dt),
        })
    return sorted(rows, key=lambda r: r["moved_at"])


# ---------- Column specs ----------

SUP_COLS = [
    Column("supplier_id", "int", nullable=False, auto=True),
    Column("name", "varchar(150)", nullable=False),
    Column("contact_name", "varchar(100)"),
    Column("phone", "varchar(30)"),
    Column("email", "varchar(255)"),
    Column("address", "varchar(300)"),
]
CAT_COLS = [
    Column("category_id", "int", nullable=False, auto=True),
    Column("name", "varchar(100)", nullable=False),
    Column("parent_id", "int"),
]
PROD_COLS = [
    Column("product_id", "int", nullable=False, auto=True),
    Column("sku", "varchar(50)", nullable=False),
    Column("name", "varchar(255)", nullable=False),
    Column("brand", "varchar(100)"),
    Column("category_id", "int"),
    Column("supplier_id", "int"),
    Column("unit", "varchar(20)", nullable=False),
    Column("cost_price", "decimal(10,2)", nullable=False),
    Column("sell_price", "decimal(10,2)", nullable=False),
    Column("vat_rate", "decimal(5,2)", nullable=False),
    Column("is_active", "smallint", nullable=False),
]
WH_COLS = [
    Column("warehouse_id", "int", nullable=False, auto=True),
    Column("name", "varchar(100)", nullable=False),
    Column("location", "varchar(255)"),
]
STOCK_COLS = [
    Column("stock_id", "int", nullable=False, auto=True),
    Column("product_id", "int", nullable=False),
    Column("warehouse_id", "int", nullable=False),
    Column("quantity", "decimal(10,3)", nullable=False),
    Column("reorder_level", "decimal(10,3)", nullable=False),
    Column("reorder_qty", "decimal(10,3)", nullable=False),
]
MOV_COLS = [
    Column("movement_id", "int", nullable=False, auto=True),
    Column("product_id", "int", nullable=False),
    Column("warehouse_id", "int", nullable=False),
    Column("movement_type", "varchar(20)", nullable=False),
    Column("quantity", "decimal(10,3)", nullable=False),
    Column("reference", "varchar(100)"),
    Column("moved_at", "datetime", nullable=False),
]


class _Supermarket(Model):
    entities: list[Entity] = []  # reference-only: no deltas

    def build_initial(self, rng: random.Random, n: int) -> list[Table]:
        random.seed(42)

        n_movements = n
        n_suppliers = _sized("suppliers", n)
        n_products = _sized("products", n)
        n_warehouses = _sized("warehouses", n)

        suppliers = gen_suppliers(n_suppliers)
        categories = gen_categories()
        products = gen_products(n_products, suppliers, categories)
        warehouses = gen_warehouses(n_warehouses)
        stock = gen_stock(products, warehouses)
        movements = gen_movements(n_movements, products, warehouses)

        return [
            Table("suppliers", SUP_COLS, suppliers),
            Table("categories", CAT_COLS, categories),
            Table("products", PROD_COLS, products),
            Table("warehouses", WH_COLS, warehouses),
            Table("stock", STOCK_COLS, stock),
            Table("stock_movements", MOV_COLS, movements),
        ]


def datagenerator() -> Model:
    return _Supermarket()

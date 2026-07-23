#
# Copyright 2026 Tabsdata Inc.
#

"""Data generator for the ``restaurant`` model.

A restaurant menu and provisioning dataset — suppliers, ingredients, menu
categories/items, recipes, dining tables, orders and order lines, purchase
orders and their lines. The size is the number of restaurant orders (the
prolific entity); every other table scales from it (menu items, ingredients,
tables and purchase orders), and suppliers derive from the ingredient count.
Reference-only: there is no delta recipe, so ``create-data`` works but
``modify-data`` has no entities to change.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from tabsdatax.itest.tdksystems2.datagen import Entity, Model
from tabsdatax.itest.tdksystems2.emit import Column, Table

_BASE_SEED = 42

# The number of orders is the size; other tables scale from it (base 500).
_PROLIFIC_BASE = 500
_SIZING_DEFAULTS = {
    "menu_items": 40,
    "ingredients": 80,
    "tables": 20,
    "po_orders": 60,
}


def _scale(default: int, n: int) -> int:
    return max(default * n // _PROLIFIC_BASE, 1)


# ---------------------------------------------------------------------------
# Pools
# ---------------------------------------------------------------------------
SUPPLIER_NAMES = [
    "La Huerta de Manolo",
    "Carnicería El Roble",
    "Pescados Frescos Mar",
    "Bodegas Rioja Select",
    "Lácteos del Norte",
    "Panadería Artesana Sol",
    "Verduras del Campo",
    "Mariscos Costa Azul",
    "Embutidos Sierra Alta",
    "Aceites Puerta del Sur",
    "Frutas Tropicales Iberia",
    "Especias del Mundo",
    "Congelados Premium SL",
    "Bio Naturales S.A.",
    "Huevos Camperos García",
]
CONTACT_FIRST = [
    "Manuel",
    "Pedro",
    "Concha",
    "Enrique",
    "Begoña",
    "Antonio",
    "Lola",
    "Paco",
    "Rosa",
    "Ramón",
    "Carmen",
    "Javier",
    "María",
    "Luis",
    "Teresa",
]
CONTACT_LAST = [
    "Ortega",
    "Ruiz",
    "Vidal",
    "Mora",
    "Arrese",
    "López",
    "García",
    "Martínez",
    "Fernández",
    "Sánchez",
    "González",
    "Rodríguez",
    "Pérez",
    "Gómez",
]

MENU_CATEGORIES = [
    ("Starters", 1),
    ("Soups", 2),
    ("Salads", 3),
    ("Pasta", 4),
    ("Rice", 5),
    ("Mains", 6),
    ("Grills", 7),
    ("Fish", 8),
    ("Desserts", 9),
    ("Beverages", 10),
]

# (name, category_idx, sell_price_lo, sell_price_hi, prep_min, is_veg, is_vegan)
MENU_ITEM_POOL = [
    # Starters (0)
    ("Bruschetta al pomodoro", 0, 6.50, 8.50, 10, True, True),
    ("Burrata con tomate", 0, 10.00, 13.00, 10, True, False),
    ("Tabla de ibéricos", 0, 14.00, 18.00, 5, False, False),
    ("Croquetas caseras (6 uds)", 0, 7.00, 9.00, 15, False, False),
    ("Tostadas con salmón", 0, 10.00, 13.00, 10, False, False),
    ("Pulpo a la gallega", 0, 12.00, 16.00, 20, False, False),
    ("Patatas bravas", 0, 5.50, 7.50, 10, True, True),
    # Soups (1)
    ("Gazpacho andaluz", 1, 5.00, 7.00, 5, True, True),
    ("Sopa de mariscos", 1, 9.00, 12.00, 20, False, False),
    ("Crema de calabaza", 1, 6.00, 8.00, 15, True, True),
    # Salads (2)
    ("Ensalada mixta", 2, 5.50, 7.00, 5, True, True),
    ("Ensalada de queso de cabra", 2, 8.00, 10.00, 8, True, False),
    ("Ensalada César con pollo", 2, 9.00, 12.00, 10, False, False),
    # Pasta (3)
    ("Spaghetti carbonara", 3, 12.00, 15.00, 15, False, False),
    ("Spaghetti al pomodoro", 3, 10.00, 13.00, 12, True, True),
    ("Penne arrabiata", 3, 10.50, 13.50, 12, True, True),
    ("Lasaña de carne", 3, 13.00, 16.00, 20, False, False),
    # Rice (4)
    ("Paella valenciana", 4, 14.00, 19.00, 25, False, False),
    ("Arroz negro con sepia", 4, 15.00, 20.00, 25, False, False),
    ("Risotto de setas", 4, 13.00, 17.00, 20, True, False),
    # Mains (5)
    ("Pollo asado con patatas", 5, 14.00, 18.00, 25, False, False),
    ("Confit de pato", 5, 18.00, 23.00, 30, False, False),
    ("Cocido madrileño", 5, 15.00, 20.00, 30, False, False),
    # Grills (6)
    ("Entrecot de ternera 250g", 6, 24.00, 32.00, 20, False, False),
    ("Chuletón de buey 400g", 6, 38.00, 50.00, 25, False, False),
    ("Pechuga de pollo a la plancha", 6, 13.00, 17.00, 15, False, False),
    ("Secreto ibérico", 6, 18.00, 24.00, 20, False, False),
    # Fish (7)
    ("Lubina a la sal", 7, 22.00, 28.00, 30, False, False),
    ("Salmón al horno con crema", 7, 19.00, 25.00, 20, False, False),
    ("Merluza en salsa verde", 7, 17.00, 22.00, 20, False, False),
    ("Bacalao al pil-pil", 7, 18.00, 23.00, 25, False, False),
    # Desserts (8)
    ("Coulant de chocolate", 8, 7.00, 9.00, 12, True, False),
    ("Tarta de queso", 8, 6.00, 8.00, 5, True, False),
    ("Crema catalana", 8, 5.50, 7.00, 5, True, False),
    ("Helado de vainilla (2 bolas)", 8, 4.00, 6.00, 2, True, False),
    ("Arroz con leche", 8, 5.00, 7.00, 5, True, False),
    ("Tiramisú", 8, 6.50, 8.50, 5, True, False),
    # Beverages (9)
    ("Vino Rioja Reserva (copa)", 9, 5.50, 7.50, 2, True, True),
    ("Agua mineral (750ml)", 9, 2.50, 3.50, 1, True, True),
    ("Refresco de cola", 9, 2.50, 3.50, 1, True, True),
    ("Cerveza de barril (caña)", 9, 2.00, 3.00, 1, True, True),
    ("Zumo de naranja natural", 9, 3.50, 5.00, 5, True, True),
]

INGREDIENT_POOL = [
    # (name, unit, cost_lo, cost_hi, allergens)
    ("Chicken breast", "kg", 5.0, 7.0, None),
    ("Salmon fillet", "kg", 13.0, 16.0, "fish"),
    ("Spaghetti", "kg", 0.9, 1.5, "gluten"),
    ("Cherry tomatoes", "kg", 2.0, 3.5, None),
    ("Garlic", "kg", 2.5, 4.0, None),
    ("Olive oil", "litre", 5.5, 8.0, None),
    ("Parmesan", "kg", 15.0, 20.0, "dairy"),
    ("Eggs", "unit", 0.2, 0.35, "eggs"),
    ("Heavy cream", "litre", 2.5, 3.5, "dairy"),
    ("Beef tenderloin", "kg", 25.0, 32.0, None),
    ("Mixed salad leaves", "kg", 3.5, 5.0, None),
    ("Lemon", "unit", 0.15, 0.30, None),
    ("Sourdough bread", "unit", 1.5, 2.2, "gluten"),
    ("Rioja Reserva wine", "litre", 4.0, 6.0, None),
    ("Still water", "litre", 0.10, 0.20, None),
    ("Vanilla ice cream", "litre", 2.8, 4.0, "dairy,eggs"),
    ("Dark chocolate", "kg", 8.0, 12.0, None),
    ("Pancetta", "kg", 10.0, 14.0, None),
    ("Pecorino romano", "kg", 14.0, 18.0, "dairy"),
    ("Asparagus", "kg", 5.0, 7.5, None),
    ("Pumpkin", "kg", 0.8, 1.5, None),
    ("Chorizo", "kg", 8.0, 12.0, None),
    ("Prawns", "kg", 12.0, 18.0, "shellfish"),
    ("Squid", "kg", 7.0, 11.0, "shellfish"),
    ("Mussels", "kg", 4.0, 6.0, "shellfish"),
    ("Duck breast", "kg", 14.0, 19.0, None),
    ("Arborio rice", "kg", 1.5, 2.5, None),
    ("Mixed mushrooms", "kg", 6.0, 10.0, None),
    ("Beef ribeye", "kg", 22.0, 30.0, None),
    ("Sea bass", "kg", 15.0, 20.0, "fish"),
    ("Hake fillet", "kg", 10.0, 14.0, "fish"),
    ("Cod fillet", "kg", 9.0, 13.0, "fish"),
    ("Milk", "litre", 0.6, 0.9, "dairy"),
    ("Butter", "kg", 7.0, 10.0, "dairy"),
    ("Flour", "kg", 0.5, 0.9, "gluten"),
    ("Sugar", "kg", 0.8, 1.2, None),
    ("Paprika", "kg", 5.0, 9.0, None),
    ("Saffron", "g", 6.0, 10.0, None),
    ("Chicken stock", "litre", 1.5, 2.5, None),
    ("Penne", "kg", 0.9, 1.5, "gluten"),
    ("Canned tomatoes", "kg", 0.8, 1.3, None),
    ("Anchovies", "kg", 12.0, 18.0, "fish"),
    ("Capers", "kg", 8.0, 14.0, None),
    ("Goat cheese", "kg", 14.0, 20.0, "dairy"),
    ("Walnuts", "kg", 10.0, 16.0, "nuts"),
    ("Honey", "kg", 6.0, 10.0, None),
    ("Vanilla bean", "unit", 1.0, 2.5, None),
    ("Mascarpone", "kg", 9.0, 13.0, "dairy"),
    ("Espresso coffee", "litre", 4.0, 8.0, None),
    ("Cocoa powder", "kg", 5.0, 9.0, None),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def rand_dt(start, end):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def fmt_date(d):
    return d.strftime("%Y-%m-%d")


def fmt_dt(d):
    return d.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------


def gen_suppliers(n):
    rows = []
    pool = SUPPLIER_NAMES * ((n // len(SUPPLIER_NAMES)) + 2)
    for i in range(1, n + 1):
        first = random.choice(CONTACT_FIRST)
        last = random.choice(CONTACT_LAST)
        rows.append({
            "supplier_id": i,
            "name": pool[i - 1],
            "contact_name": f"{first} {last}",
            "phone": f"+34 91 555 {random.randint(1000, 9999)}",
            "email": f"{last.lower()}{i}@{pool[i - 1].split()[0].lower()}.es",
            "address": (
                f"Mercado {random.choice(['Central', 'Norte', 'Sur'])}, "
                f"Puesto {random.randint(1, 30)}"
            ),
        })
    return rows


def gen_ingredients(n, suppliers):
    rows = []
    pool = INGREDIENT_POOL * ((n // len(INGREDIENT_POOL)) + 2)
    for i in range(1, n + 1):
        name, unit, clo, chi, allergens = pool[i - 1]
        suffix = f" #{i}" if i > len(INGREDIENT_POOL) else ""
        cost = round(random.uniform(clo, chi), 4)
        rows.append({
            "ingredient_id": i,
            "name": name + suffix,
            "unit": unit,
            "unit_cost": cost,
            "stock_qty": round(random.uniform(0, 50), 3),
            "reorder_level": round(random.uniform(2, 15), 3),
            "supplier_id": random.choice(suppliers)["supplier_id"],
            "allergen_flags": allergens,
        })
    return rows


def gen_menu_categories():
    return [
        {"category_id": i, "name": name, "display_order": order}
        for i, (name, order) in enumerate(MENU_CATEGORIES, 1)
    ]


def gen_menu_items(n, categories):
    rows = []
    pool = MENU_ITEM_POOL * ((n // len(MENU_ITEM_POOL)) + 2)
    cat_list = categories
    for i in range(1, n + 1):
        name, cat_idx, plo, phi, prep, is_veg, is_vegan = pool[i - 1]
        suffix = f" #{i}" if i > len(MENU_ITEM_POOL) else ""
        cat_id = cat_list[cat_idx % len(cat_list)]["category_id"]
        rows.append({
            "item_id": i,
            "name": name + suffix,
            "description": "Traditional recipe, freshly prepared.",
            "category_id": cat_id,
            "sell_price": round(random.uniform(plo, phi), 2),
            "prep_time_min": prep,
            "is_available": 1 if random.random() < 0.92 else 0,
            "is_vegetarian": 1 if is_veg else 0,
            "is_vegan": 1 if is_vegan else 0,
        })
    return rows


def gen_recipes(menu_items, ingredients):
    """Assign 2-6 ingredients to each menu item."""
    rows = []
    rid = 1
    for item in menu_items:
        n_ing = random.randint(2, 6)
        selected = random.sample(ingredients, min(n_ing, len(ingredients)))
        used = set()
        for ing in selected:
            if ing["ingredient_id"] in used:
                continue
            used.add(ing["ingredient_id"])
            qty = round(random.uniform(0.01, 0.5), 4)
            rows.append({
                "recipe_id": rid,
                "item_id": item["item_id"],
                "ingredient_id": ing["ingredient_id"],
                "quantity": qty,
            })
            rid += 1
    return rows


def gen_tables(n):
    rows = []
    locations = [
        "indoor",
        "indoor",
        "indoor",
        "indoor",
        "terrace",
        "terrace",
        "private",
    ]
    for i in range(1, n + 1):
        rows.append({
            "table_id": i,
            "table_no": i,
            "capacity": random.choice([2, 2, 4, 4, 4, 6, 6, 8]),
            "location": random.choice(locations),
        })
    return rows


def gen_orders(n, tables):
    rows = []
    start = datetime(2024, 1, 1, 12, 0, 0)
    end = datetime(2024, 12, 31, 23, 0, 0)
    statuses = ["paid", "paid", "paid", "paid", "open", "billed", "void"]
    methods = ["card", "card", "card", "cash", "voucher"]
    for i in range(1, n + 1):
        tbl = random.choice(tables)
        opened = rand_dt(start, end)
        # Round to nearest 15 min
        opened = opened.replace(
            second=0, microsecond=0, minute=(opened.minute // 15) * 15
        )
        status = random.choice(statuses)
        closed = None
        method = None
        if status in ("paid", "billed"):
            duration = timedelta(minutes=random.randint(45, 150))
            closed = opened + duration
            method = random.choice(methods) if status == "paid" else None
        covers = random.randint(1, tbl["capacity"])
        rows.append({
            "order_id": i,
            "table_id": tbl["table_id"],
            "opened_at": fmt_dt(opened),
            "closed_at": fmt_dt(closed) if closed else None,
            "covers": covers,
            "status": status,
            "total_amount": None,  # filled after lines
            "payment_method": method,
            "_covers": covers,
        })
    return sorted(rows, key=lambda r: r["opened_at"])


def gen_order_lines(orders, menu_items):
    """Generate 2-8 lines per order, compute totals."""
    rows = []
    lid = 1
    available_items = [m for m in menu_items if m["is_available"]]
    item_notes = [
        None,
        None,
        None,
        None,
        "No sauce please",
        "Extra spicy",
        "Well done",
        "Allergy: no nuts",
        "Half portion",
        "Extra cheese",
    ]
    for order in orders:
        n_lines = random.randint(2, min(8, order["_covers"] * 3))
        selected = random.choices(available_items, k=n_lines)
        order_total = 0.0
        for item in selected:
            qty = random.randint(1, 3)
            price = item["sell_price"]
            order_total += qty * price
            rows.append({
                "line_id": lid,
                "order_id": order["order_id"],
                "item_id": item["item_id"],
                "quantity": qty,
                "unit_price": price,
                "notes": random.choice(item_notes),
            })
            lid += 1
        if order["status"] in ("paid", "billed"):
            order["total_amount"] = round(order_total, 2)
    return rows


def gen_purchase_orders(n, suppliers):
    rows = []
    start = datetime(2024, 1, 1)
    end = datetime(2024, 12, 15)
    statuses = ["received", "received", "received", "sent", "partial", "cancelled"]
    for i in range(1, n + 1):
        created = rand_dt(start, end)
        exp = created + timedelta(days=random.randint(1, 5))
        status = random.choice(statuses)
        received = None
        if status in ("received", "partial"):
            received = exp + timedelta(days=random.randint(0, 2))
        rows.append({
            "po_id": i,
            "supplier_id": random.choice(suppliers)["supplier_id"],
            "created_at": fmt_dt(created),
            "expected_date": fmt_date(exp),
            "received_date": fmt_date(received) if received else None,
            "status": status,
            "total_cost": None,  # filled after lines
        })
    return rows


def gen_po_lines(purchase_orders, ingredients):
    rows = []
    lid = 1
    for po in purchase_orders:
        n_lines = random.randint(2, 8)
        selected = random.sample(ingredients, min(n_lines, len(ingredients)))
        po_total = 0.0
        for ing in selected:
            qty_ord = round(random.uniform(5, 50), 3)
            qty_rec = 0.0
            if po["status"] == "received":
                qty_rec = qty_ord
            elif po["status"] == "partial":
                qty_rec = round(qty_ord * random.uniform(0.3, 0.8), 3)
            cost = ing["unit_cost"]
            po_total += qty_ord * cost
            rows.append({
                "po_line_id": lid,
                "po_id": po["po_id"],
                "ingredient_id": ing["ingredient_id"],
                "qty_ordered": qty_ord,
                "qty_received": qty_rec,
                "unit_cost": cost,
            })
            lid += 1
        po["total_cost"] = round(po_total, 2)
    return rows


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SUPPLIER_COLS = [
    Column("supplier_id", "int", nullable=False, auto=True),
    Column("name", "varchar(150)", nullable=False),
    Column("contact_name", "varchar(100)"),
    Column("phone", "varchar(30)"),
    Column("email", "varchar(255)"),
    Column("address", "varchar(300)"),
]
INGREDIENT_COLS = [
    Column("ingredient_id", "int", nullable=False, auto=True),
    Column("name", "varchar(150)", nullable=False),
    Column("unit", "varchar(20)", nullable=False),
    Column("unit_cost", "decimal(10,4)", nullable=False),
    Column("stock_qty", "decimal(10,3)", nullable=False),
    Column("reorder_level", "decimal(10,3)", nullable=False),
    Column("supplier_id", "int"),
    Column("allergen_flags", "varchar(100)"),
]
MENUCAT_COLS = [
    Column("category_id", "int", nullable=False, auto=True),
    Column("name", "varchar(100)", nullable=False),
    Column("display_order", "smallint", nullable=False),
]
MENUITEM_COLS = [
    Column("item_id", "int", nullable=False, auto=True),
    Column("name", "varchar(150)", nullable=False),
    Column("description", "text"),
    Column("category_id", "int", nullable=False),
    Column("sell_price", "decimal(8,2)", nullable=False),
    Column("prep_time_min", "smallint"),
    Column("is_available", "smallint", nullable=False),
    Column("is_vegetarian", "smallint", nullable=False),
    Column("is_vegan", "smallint", nullable=False),
]
RECIPE_COLS = [
    Column("recipe_id", "int", nullable=False, auto=True),
    Column("item_id", "int", nullable=False),
    Column("ingredient_id", "int", nullable=False),
    Column("quantity", "decimal(10,4)", nullable=False),
]
TABLE_COLS = [
    Column("table_id", "int", nullable=False, auto=True),
    Column("table_no", "int", nullable=False),
    Column("capacity", "int", nullable=False),
    Column("location", "varchar(20)", nullable=False),
]
ORDER_COLS = [
    Column("order_id", "int", nullable=False, auto=True),
    Column("table_id", "int", nullable=False),
    Column("opened_at", "datetime", nullable=False),
    Column("closed_at", "datetime"),
    Column("covers", "smallint", nullable=False),
    Column("status", "varchar(20)", nullable=False),
    Column("total_amount", "decimal(10,2)"),
    Column("payment_method", "varchar(20)"),
]
ORDER_LINE_COLS = [
    Column("line_id", "int", nullable=False, auto=True),
    Column("order_id", "int", nullable=False),
    Column("item_id", "int", nullable=False),
    Column("quantity", "smallint", nullable=False),
    Column("unit_price", "decimal(8,2)", nullable=False),
    Column("notes", "varchar(200)"),
]
PO_COLS = [
    Column("po_id", "int", nullable=False, auto=True),
    Column("supplier_id", "int", nullable=False),
    Column("created_at", "datetime", nullable=False),
    Column("expected_date", "date"),
    Column("received_date", "date"),
    Column("status", "varchar(20)", nullable=False),
    Column("total_cost", "decimal(10,2)"),
]
PO_LINE_COLS = [
    Column("po_line_id", "int", nullable=False, auto=True),
    Column("po_id", "int", nullable=False),
    Column("ingredient_id", "int", nullable=False),
    Column("qty_ordered", "decimal(10,3)", nullable=False),
    Column("qty_received", "decimal(10,3)", nullable=False),
    Column("unit_cost", "decimal(10,4)", nullable=False),
]


class _Restaurant(Model):
    entities: list[Entity] = []  # reference-only: no deltas

    def build_initial(self, rng: random.Random, n: int) -> list[Table]:
        random.seed(_BASE_SEED)
        n_menu_items = _scale(_SIZING_DEFAULTS["menu_items"], n)
        n_ingredients = _scale(_SIZING_DEFAULTS["ingredients"], n)
        n_tables = _scale(_SIZING_DEFAULTS["tables"], n)
        n_po_orders = _scale(_SIZING_DEFAULTS["po_orders"], n)

        n_suppliers = max(5, n_ingredients // 10)
        suppliers = gen_suppliers(n_suppliers)
        ingredients = gen_ingredients(n_ingredients, suppliers)
        categories = gen_menu_categories()
        menu_items = gen_menu_items(n_menu_items, categories)
        recipes = gen_recipes(menu_items, ingredients)
        tables_ = gen_tables(n_tables)
        orders = gen_orders(n, tables_)
        order_lines = gen_order_lines(orders, menu_items)
        purchase_orders = gen_purchase_orders(n_po_orders, suppliers)
        po_lines = gen_po_lines(purchase_orders, ingredients)

        for o in orders:
            o.pop("_covers", None)

        return [
            Table("suppliers", SUPPLIER_COLS, suppliers),
            Table("ingredients", INGREDIENT_COLS, ingredients),
            Table("menu_categories", MENUCAT_COLS, categories),
            Table("menu_items", MENUITEM_COLS, menu_items),
            Table("recipes", RECIPE_COLS, recipes),
            Table("tables", TABLE_COLS, tables_),
            Table("orders", ORDER_COLS, orders),
            Table("order_lines", ORDER_LINE_COLS, order_lines),
            Table("purchase_orders", PO_COLS, purchase_orders),
            Table("po_lines", PO_LINE_COLS, po_lines),
        ]


def datagenerator() -> Model:
    return _Restaurant()

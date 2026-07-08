#!/usr/bin/env python3
#
# Copyright 2026 Tabsdata Inc.
#

"""Finance — subscription billing data keyed to Salesforce Accounts.

This is the *finance* half of the Salesforce ↔ Finance pair. The
companion ``salesforce`` model loads matching Accounts + Opportunities
into a Salesforce dev org via Snowfakery + CumulusCI; this model
produces a finance billing schema (``customers`` / ``payment_methods``
/ ``subscription_plans`` / ``customer_subscriptions`` /
``customer_subscription_events``) where each ``customer`` row carries
the matching ``salesforce_account_id`` (the 18-char SF Id), so finance
state can be joined back to the SF pipeline for forecasting.

The model is **fixed-data** — it ships with a snapshot of 351 SF
Accounts in ``accounts.csv`` and emits one ``customer`` per account
deterministically (per-account RNG seeded by the SF Id). Each row's
contents (plan tier, payment methods, subscription status, event
history) is reproducible across runs.

DDL is MySQL-only — no per-system override is shipped (Postgres /
Oracle / etc. would need their own DDL adapted from ``mysql.ddl``).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tabsdatax.itest.tdksystems.emit import VALID_FORMATS, Column, Table, write_tables

HERE = Path(__file__).resolve().parent
ACCOUNTS_CSV = HERE / "accounts.csv"

# Frozen "now" so repeat runs produce identical timestamps.
NOW = datetime(2026, 4, 19, 12, 0, 0, tzinfo=timezone.utc)

COUNTRY_TO_ISO2 = {
    "United States": "US",
    "USA": "US",
    "Canada": "CA",
    "United Kingdom": "GB",
    "Germany": "DE",
    "France": "FR",
    "Spain": "ES",
    "Australia": "AU",
}

# (plan_code, name, description, interval, interval_count, trial_days, price_usd)
PLANS = [
    (
        "STARTER_M",
        "Starter (Monthly)",
        "Entry tier for small teams.",
        "MONTH",
        1,
        14,
        29.00,
    ),
    (
        "PRO_M",
        "Pro (Monthly)",
        "Growing teams, standard support.",
        "MONTH",
        1,
        0,
        99.00,
    ),
    (
        "BUSINESS_M",
        "Business (Monthly)",
        "Mid-market, priority support.",
        "MONTH",
        1,
        0,
        499.00,
    ),
    (
        "ENTERPRISE_Y",
        "Enterprise (Annual)",
        "Enterprise, SLA, custom terms.",
        "YEAR",
        1,
        0,
        12000.00,
    ),
    (
        "SCALE_Y",
        "Scale (Annual)",
        "Annual plan with volume discount.",
        "YEAR",
        1,
        0,
        4800.00,
    ),
]

STATUS_WEIGHTS = [
    ("ACTIVE", 0.68),
    ("TRIALING", 0.08),
    ("PAST_DUE", 0.05),
    ("PAUSED", 0.03),
    ("CANCELED", 0.10),
    ("EXPIRED", 0.04),
    ("INCOMPLETE", 0.02),
]
PAYMENT_METHOD_WEIGHTS = [
    ("CARD", 0.75),
    ("ACH", 0.15),
    ("WIRE", 0.07),
    ("SEPA", 0.03),
]
CARD_BRANDS = ["VISA", "MASTERCARD", "AMEX", "DISCOVER"]

SEED = 42  # overridden by --seed (tdx framework doesn't pass --seed; left
# for direct CLI use of this script).


# ---- helpers --------------------------------------------------------------


def _rng(sf_id: str, salt: str) -> random.Random:
    key = f"{SEED}:{sf_id}:{salt}".encode()
    digest = hashlib.sha256(key).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def _pick(rng: random.Random, choices: list[tuple[str, float]]) -> str:
    r = rng.random()
    acc = 0.0
    for name, w in choices:
        acc += w
        if r <= acc:
            return name
    return choices[-1][0]


def _slug_email(name: str, rng: random.Random) -> str:
    base = "".join(c.lower() if c.isalnum() else "" for c in name)[:24] or "customer"
    domains = ["example.com", "acme-mail.test", "corp.invalid", "billing.test"]
    return f"billing+{base}@{rng.choice(domains)}"


def _plan_index(
    revenue: float | None, employees: int | None, rng: random.Random
) -> int:
    score = 0
    if revenue:
        if revenue >= 250_000_000:
            score += 3
        elif revenue >= 100_000_000:
            score += 2
        elif revenue >= 25_000_000:
            score += 1
    if employees:
        if employees >= 2500:
            score += 3
        elif employees >= 1000:
            score += 2
        elif employees >= 250:
            score += 1
    score += rng.randint(-1, 1)
    if score >= 5:
        return 3  # ENTERPRISE_Y
    if score >= 3:
        return 4  # SCALE_Y
    if score >= 2:
        return 2  # BUSINESS_M
    if score >= 1:
        return 1  # PRO_M
    return 0  # STARTER_M


def _parse_sf_dt(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z").astimezone(timezone.utc)


def _to_naive_utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


# ---- record shapes --------------------------------------------------------


@dataclass
class _Plan:
    plan_id: int
    price_id: int
    plan_code: str
    name: str
    description: str
    interval: str
    interval_count: int
    trial_days: int
    currency: str
    amount: float


def _build_plans() -> list[_Plan]:
    return [
        _Plan(
            plan_id=i,
            price_id=i,
            plan_code=code,
            name=name,
            description=desc,
            interval=interval,
            interval_count=count,
            trial_days=trial,
            currency="USD",
            amount=amount,
        )
        for i, (code, name, desc, interval, count, trial, amount) in enumerate(
            PLANS, start=1
        )
    ]


def _compute_period(
    started: datetime, plan: _Plan
) -> tuple[datetime, datetime, datetime]:
    if plan.interval == "YEAR":
        delta = timedelta(days=365 * plan.interval_count)
    elif plan.interval == "MONTH":
        delta = timedelta(days=30 * plan.interval_count)
    elif plan.interval == "WEEK":
        delta = timedelta(days=7 * plan.interval_count)
    else:
        delta = timedelta(days=plan.interval_count)
    period_start = started
    while period_start + delta < NOW:
        period_start += delta
    period_end = period_start + delta
    return period_start, period_end, period_end


# ---- table builders -------------------------------------------------------

PLAN_COLUMNS = [
    Column("plan_id", "bigint", auto=True),
    Column("plan_code", "varchar(64)"),
    Column("name", "varchar(255)"),
    Column("description", "text"),
    Column("billing_interval", "varchar(16)"),
    Column("billing_interval_count", "smallint"),
    Column("trial_period_days", "smallint"),
    Column("is_active", "boolean"),
]
PRICE_COLUMNS = [
    Column("price_id", "bigint", auto=True),
    Column("plan_id", "bigint"),
    Column("currency", "char(3)"),
    Column("amount", "decimal(19,4)"),
    Column("effective_from", "datetime"),
    Column("effective_to", "datetime", nullable=True),
    Column("is_active", "boolean"),
]
CUSTOMER_COLUMNS = [
    Column("customer_id", "bigint", auto=True),
    Column("salesforce_account_id", "varchar(18)"),
    Column("name", "varchar(255)"),
    Column("email", "varchar(320)"),
    Column("phone", "varchar(64)", nullable=True),
    Column("country", "char(2)"),
    Column("default_currency", "char(3)"),
    Column("is_active", "boolean"),
    Column("created_at", "datetime"),
]
PAYMENT_METHOD_COLUMNS = [
    Column("payment_method_id", "bigint", auto=True),
    Column("customer_id", "bigint"),
    Column("method_type", "varchar(32)"),
    Column("brand", "varchar(32)", nullable=True),
    Column("last_four", "char(4)", nullable=True),
    Column("exp_month", "smallint", nullable=True),
    Column("exp_year", "smallint", nullable=True),
    Column("billing_country", "char(2)"),
    Column("is_default", "boolean"),
    Column("is_active", "boolean"),
    Column("created_at", "datetime"),
]
SUBSCRIPTION_COLUMNS = [
    Column("subscription_id", "bigint", auto=True),
    Column("customer_id", "bigint"),
    Column("plan_id", "bigint"),
    Column("price_id", "bigint"),
    Column("currency", "char(3)"),
    Column("unit_amount", "decimal(19,4)"),
    Column("quantity", "int"),
    Column("discount_amount", "decimal(19,4)"),
    Column("tax_amount", "decimal(19,4)"),
    Column("total_amount", "decimal(19,4)"),
    Column("status", "varchar(32)"),
    Column("started_at", "datetime"),
    Column("trial_start", "datetime", nullable=True),
    Column("trial_end", "datetime", nullable=True),
    Column("current_period_start", "datetime"),
    Column("current_period_end", "datetime"),
    Column("next_billing_at", "datetime", nullable=True),
    Column("cancel_at", "datetime", nullable=True),
    Column("canceled_at", "datetime", nullable=True),
    Column("ended_at", "datetime", nullable=True),
    Column("cancel_at_period_end", "boolean"),
    Column("payment_method_id", "bigint", nullable=True),
    Column("created_at", "datetime"),
]
EVENT_COLUMNS = [
    Column("event_id", "bigint", auto=True),
    Column("subscription_id", "bigint"),
    Column("event_type", "varchar(64)"),
    Column("from_status", "varchar(32)", nullable=True),
    Column("to_status", "varchar(32)", nullable=True),
    Column("occurred_at", "datetime"),
    Column("actor", "varchar(128)"),
]


def _build_tables(accounts: list[dict]) -> list[Table]:
    plans = _build_plans()

    # subscription_plans
    plan_rows = [
        {
            "plan_id": p.plan_id,
            "plan_code": p.plan_code,
            "name": p.name,
            "description": p.description,
            "billing_interval": p.interval,
            "billing_interval_count": p.interval_count,
            "trial_period_days": p.trial_days,
            "is_active": True,
        }
        for p in plans
    ]
    # subscription_plan_prices
    effective_from = _to_naive_utc(NOW - timedelta(days=365))
    price_rows = [
        {
            "price_id": p.price_id,
            "plan_id": p.plan_id,
            "currency": p.currency,
            "amount": p.amount,
            "effective_from": effective_from,
            "effective_to": None,
            "is_active": True,
        }
        for p in plans
    ]
    # customers
    customer_rows: list[dict] = []
    customer_ids: list[int] = []
    for idx, a in enumerate(accounts, start=1):
        rng = _rng(a["Id"], "customer")
        country = COUNTRY_TO_ISO2.get(a.get("BillingCountry", "").strip(), "US")
        customer_rows.append({
            "customer_id": idx,
            "salesforce_account_id": a["Id"],
            "name": a["Name"],
            "email": _slug_email(a["Name"], rng),
            "phone": a.get("Phone") or None,
            "country": country,
            "default_currency": "USD",
            "is_active": rng.random() > 0.05,
            "created_at": _to_naive_utc(_parse_sf_dt(a["CreatedDate"])),
        })
        customer_ids.append(idx)

    # payment_methods
    pm_rows: list[dict] = []
    default_pm: dict[int, int] = {}
    pm_id = 0
    for a, cid in zip(accounts, customer_ids):
        rng = _rng(a["Id"], "pm")
        country = COUNTRY_TO_ISO2.get(a.get("BillingCountry", "").strip(), "US")
        count = rng.choices([1, 2, 3], weights=[0.55, 0.35, 0.10])[0]
        for i in range(count):
            pm_id += 1
            method = _pick(rng, PAYMENT_METHOD_WEIGHTS)
            brand = rng.choice(CARD_BRANDS) if method == "CARD" else None
            last_four = f"{rng.randint(0, 9999):04d}" if method == "CARD" else None
            exp_m = rng.randint(1, 12) if method == "CARD" else None
            exp_y = rng.randint(2026, 2031) if method == "CARD" else None
            pm_rows.append({
                "payment_method_id": pm_id,
                "customer_id": cid,
                "method_type": method,
                "brand": brand,
                "last_four": last_four,
                "exp_month": exp_m,
                "exp_year": exp_y,
                "billing_country": country,
                "is_default": (i == 0),
                "is_active": True,
                "created_at": _to_naive_utc(NOW - timedelta(days=rng.randint(30, 700))),
            })
            if i == 0:
                default_pm[cid] = pm_id

    # customer_subscriptions + customer_subscription_events
    sub_rows: list[dict] = []
    event_rows: list[dict] = []
    for idx, (a, cid) in enumerate(zip(accounts, customer_ids), start=1):
        rng = _rng(a["Id"], "sub")
        revenue = float(a["AnnualRevenue"]) if a.get("AnnualRevenue") else None
        employees = (
            int(float(a["NumberOfEmployees"])) if a.get("NumberOfEmployees") else None
        )
        plan = plans[_plan_index(revenue, employees, rng)]

        account_created = _parse_sf_dt(a["CreatedDate"])
        max_start = NOW - timedelta(days=1)
        if account_created >= max_start:
            started = account_created
        else:
            span_days = max(1, (max_start - account_created).days)
            started = account_created + timedelta(days=rng.randint(0, span_days))

        status = _pick(rng, STATUS_WEIGHTS)

        trial_start = None
        trial_end = None
        if plan.trial_days > 0 and rng.random() < 0.30:
            trial_start = started
            trial_end = started + timedelta(days=plan.trial_days)

        quantity = rng.choices(
            [1, 1, 1, 2, 5, 10, 25], weights=[0.55, 0.15, 0.10, 0.08, 0.06, 0.04, 0.02]
        )[0]
        unit_amount = plan.amount
        discount_rate = rng.choice([0, 0, 0, 0.05, 0.10, 0.15])
        discount_amount = round(unit_amount * quantity * discount_rate, 4)
        taxable = unit_amount * quantity - discount_amount
        tax_rate = rng.choice([0, 0.05, 0.0725, 0.08])
        tax_amount = round(taxable * tax_rate, 4)
        total_amount = round(taxable + tax_amount, 4)

        period_start, period_end, next_billing = _compute_period(started, plan)

        cancel_at = None
        canceled_at = None
        ended_at = None
        cancel_at_period_end = False
        if status == "CANCELED":
            canceled_at = period_start + timedelta(days=rng.randint(0, 20))
            ended_at = canceled_at
            next_billing = None
        elif status == "EXPIRED":
            ended_at = period_end - timedelta(days=1)
            next_billing = None
        elif status in ("PAUSED", "INCOMPLETE"):
            next_billing = None
        elif status == "TRIALING" and trial_end:
            next_billing = trial_end
        elif status == "ACTIVE" and rng.random() < 0.15:
            cancel_at_period_end = True
            cancel_at = period_end

        if status == "TRIALING" and not trial_end:
            trial_start = started
            trial_end = started + timedelta(days=max(plan.trial_days, 7))
            next_billing = trial_end

        sub_rows.append({
            "subscription_id": idx,
            "customer_id": cid,
            "plan_id": plan.plan_id,
            "price_id": plan.price_id,
            "currency": plan.currency,
            "unit_amount": unit_amount,
            "quantity": quantity,
            "discount_amount": discount_amount,
            "tax_amount": tax_amount,
            "total_amount": total_amount,
            "status": status,
            "started_at": _to_naive_utc(started),
            "trial_start": _to_naive_utc(trial_start) if trial_start else None,
            "trial_end": _to_naive_utc(trial_end) if trial_end else None,
            "current_period_start": _to_naive_utc(period_start),
            "current_period_end": _to_naive_utc(period_end),
            "next_billing_at": _to_naive_utc(next_billing) if next_billing else None,
            "cancel_at": _to_naive_utc(cancel_at) if cancel_at else None,
            "canceled_at": _to_naive_utc(canceled_at) if canceled_at else None,
            "ended_at": _to_naive_utc(ended_at) if ended_at else None,
            "cancel_at_period_end": cancel_at_period_end,
            "payment_method_id": default_pm.get(cid),
            "created_at": _to_naive_utc(started),
        })

        # Events
        def _evt(
            etype: str, fr: str | None, to: str | None, when: datetime, actor: str
        ) -> None:
            event_rows.append({
                "subscription_id": idx,
                "event_type": etype,
                "from_status": fr,
                "to_status": to,
                "occurred_at": _to_naive_utc(when),
                "actor": actor,
            })

        _evt("CREATED", None, "INCOMPLETE", started, "system")
        activation = "TRIALING" if trial_end else "ACTIVE"
        _evt(
            "ACTIVATED",
            "INCOMPLETE",
            activation,
            started + timedelta(minutes=1),
            "system",
        )
        if trial_end and status not in ("INCOMPLETE",) and status != "TRIALING":
            _evt("RENEWED", "TRIALING", status, trial_end, "system")
        if status in ("ACTIVE", "PAST_DUE", "PAUSED"):
            cursor = started + timedelta(days=plan.trial_days if trial_end else 0)
            delta_days = 365 if plan.interval == "YEAR" else 30
            while cursor + timedelta(days=delta_days) < period_start:
                cursor = cursor + timedelta(days=delta_days)
                _evt("RENEWED", "ACTIVE", "ACTIVE", cursor, "system")
        if status == "PAST_DUE":
            _evt(
                "PAYMENT_FAILED",
                "ACTIVE",
                "PAST_DUE",
                period_start + timedelta(days=3),
                "webhook:stripe",
            )
        if status == "PAUSED":
            _evt(
                "PAUSED",
                "ACTIVE",
                "PAUSED",
                period_start + timedelta(days=5),
                "user:admin",
            )
        if cancel_at_period_end:
            _evt(
                "CANCEL_SCHEDULED",
                "ACTIVE",
                "ACTIVE",
                period_start + timedelta(days=2),
                "user:admin",
            )
        if status == "CANCELED" and canceled_at:
            _evt("CANCELED", "ACTIVE", "CANCELED", canceled_at, "user:admin")
        if status == "EXPIRED" and ended_at:
            _evt("EXPIRED", "ACTIVE", "EXPIRED", ended_at, "system")

    return [
        Table(name="subscription_plans", columns=PLAN_COLUMNS, rows=plan_rows),
        Table(name="subscription_plan_prices", columns=PRICE_COLUMNS, rows=price_rows),
        Table(name="customers", columns=CUSTOMER_COLUMNS, rows=customer_rows),
        Table(name="payment_methods", columns=PAYMENT_METHOD_COLUMNS, rows=pm_rows),
        Table(
            name="customer_subscriptions", columns=SUBSCRIPTION_COLUMNS, rows=sub_rows
        ),
        Table(
            name="customer_subscription_events", columns=EVENT_COLUMNS, rows=event_rows
        ),
    ]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--format", required=True, choices=VALID_FORMATS)
    p.add_argument("--out", required=True)
    p.add_argument("--chunks", type=int, default=None)
    args = p.parse_args()

    with ACCOUNTS_CSV.open(newline="") as fh:
        accounts = list(csv.DictReader(fh))
    if not accounts:
        raise SystemExit(f"ERROR: no rows in {ACCOUNTS_CSV}")

    tables = _build_tables(accounts)
    write_tables(
        tables,
        args.out,
        args.format,
        source_dir=HERE,
        chunks=args.chunks,
    )


if __name__ == "__main__":
    main()

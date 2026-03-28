# Copyright © 2026 Pathway
#
# ETL pipeline that reads from RabbitMQ Streams, validates, transforms,
# aggregates, joins, and writes enriched results to PostgreSQL.
#
# This single Pathway process replaces what traditionally required:
# - Celery/RabbitMQ workers (consuming)
# - Staging databases (validation)
# - Airflow DAGs (orchestration)
# - Separate Python/Java workers (transformation)

import re
import time

import pathway as pw

# To use advanced features with Pathway Scale, get your free license key from
# https://pathway.com/features and paste it below.
# To use Pathway Community, comment out the line below.
# pw.set_license_key("demo-license-key-with-telemetry")

RABBITMQ_URI = "rabbitmq-stream://guest:guest@0.0.0.0:5552"

postgres_settings = {
    "host": "0.0.0.0",
    "port": "5433",
    "dbname": "etl_db",
    "user": "pathway",
    "password": "pathway",
}


# ── Schemas ──────────────────────────────────────────────────────────────────


class EmployeeSchema(pw.Schema):
    employee_id: int
    first_name: str
    last_name: str
    email: str
    phone: str
    department: str
    salary: str
    hire_date: str
    status: str


class OrderSchema(pw.Schema):
    order_id: int
    employee_id: int
    amount: str
    order_date: str
    product: str
    quantity: int


class ListingSchema(pw.Schema):
    listing_id: int
    title: str
    price: str
    location: str
    source: str
    posted_date: str
    agent_employee_id: int


# ── Validation UDFs ──────────────────────────────────────────────────────────


@pw.udf
def is_valid_email(email: str) -> bool:
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email))


@pw.udf
def is_valid_phone(phone: str) -> bool:
    digits = re.sub(r"[\s\-\(\)\+]", "", phone)
    return len(digits) >= 7 and digits.isdigit()


@pw.udf
def is_positive_number(value: str) -> bool:
    try:
        return float(value) > 0
    except (ValueError, TypeError):
        return False


# ── Transformation UDFs ──────────────────────────────────────────────────────


@pw.udf
def normalize_lower(value: str) -> str:
    return value.strip().lower()


@pw.udf
def parse_float(value: str) -> float:
    return float(value)


# ── READ from RabbitMQ ───────────────────────────────────────────────────────

employees = pw.io.rabbitmq.read(
    RABBITMQ_URI,
    "employees_raw",
    schema=EmployeeSchema,
    format="json",
    autocommit_duration_ms=500,
)

orders = pw.io.rabbitmq.read(
    RABBITMQ_URI,
    "orders_raw",
    schema=OrderSchema,
    format="json",
    autocommit_duration_ms=500,
)

listings = pw.io.rabbitmq.read(
    RABBITMQ_URI,
    "listings_raw",
    schema=ListingSchema,
    format="json",
    autocommit_duration_ms=500,
)

# ── VALIDATE employees ───────────────────────────────────────────────────────

valid_employees = employees.filter(
    (pw.this.first_name != "")
    & is_valid_email(pw.this.email)
    & is_valid_phone(pw.this.phone)
    & is_positive_number(pw.this.salary)
)

# ── TRANSFORM employees ─────────────────────────────────────────────────────

transformed_employees = valid_employees.select(
    employee_id=pw.this.employee_id,
    full_name=pw.this.first_name + " " + pw.this.last_name,
    email=normalize_lower(pw.this.email),
    phone=pw.this.phone,
    department=normalize_lower(pw.this.department),
    salary=parse_float(pw.this.salary),
    hire_date=pw.this.hire_date,
    is_active=(pw.this.status == "active"),
)

# ── VALIDATE & TRANSFORM orders ─────────────────────────────────────────────

valid_orders = orders.filter(
    is_positive_number(pw.this.amount) & (pw.this.quantity > 0)
)

transformed_orders = valid_orders.select(
    order_id=pw.this.order_id,
    employee_id=pw.this.employee_id,
    amount=parse_float(pw.this.amount),
    order_date=pw.this.order_date,
    product=pw.this.product,
    quantity=pw.this.quantity,
    line_total=parse_float(pw.this.amount) * pw.this.quantity,
)

# ── VALIDATE & TRANSFORM listings ───────────────────────────────────────────

valid_listings = listings.filter(
    (pw.this.title != "") & is_positive_number(pw.this.price)
)

transformed_listings = valid_listings.select(
    listing_id=pw.this.listing_id,
    title=pw.this.title,
    price=parse_float(pw.this.price),
    location=pw.this.location,
    source=pw.this.source,
    posted_date=pw.this.posted_date,
    agent_employee_id=pw.this.agent_employee_id,
)

# ── AGGREGATE orders per employee ────────────────────────────────────────────

order_stats = transformed_orders.groupby(pw.this.employee_id).reduce(
    employee_id=pw.this.employee_id,
    total_orders=pw.reducers.count(),
    total_spent=pw.reducers.sum(pw.this.line_total),
)

# ── AGGREGATE listings per agent ─────────────────────────────────────────────

listing_stats = transformed_listings.groupby(pw.this.agent_employee_id).reduce(
    agent_employee_id=pw.this.agent_employee_id,
    total_listings=pw.reducers.count(),
    avg_listing_price=pw.reducers.avg(pw.this.price),
)

# ── JOIN: enrich employees with order stats ──────────────────────────────────

employees_with_orders = transformed_employees.join_left(
    order_stats, pw.left.employee_id == pw.right.employee_id
).select(
    employee_id=pw.left.employee_id,
    full_name=pw.left.full_name,
    email=pw.left.email,
    phone=pw.left.phone,
    department=pw.left.department,
    salary=pw.left.salary,
    hire_date=pw.left.hire_date,
    is_active=pw.left.is_active,
    total_orders=pw.coalesce(pw.right.total_orders, 0),
    total_spent=pw.coalesce(pw.right.total_spent, 0.0),
)

# ── JOIN: enrich employees with listing stats ────────────────────────────────

enriched_employees = employees_with_orders.join_left(
    listing_stats, pw.left.employee_id == pw.right.agent_employee_id
).select(
    employee_id=pw.left.employee_id,
    full_name=pw.left.full_name,
    email=pw.left.email,
    phone=pw.left.phone,
    department=pw.left.department,
    salary=pw.left.salary,
    hire_date=pw.left.hire_date,
    is_active=pw.left.is_active,
    total_orders=pw.left.total_orders,
    total_spent=pw.left.total_spent,
    total_listings=pw.coalesce(pw.right.total_listings, 0),
    avg_listing_price=pw.coalesce(pw.right.avg_listing_price, 0.0),
)

# ── JOIN: enrich listings with agent name ────────────────────────────────────

enriched_listings = transformed_listings.join_left(
    transformed_employees, pw.left.agent_employee_id == pw.right.employee_id
).select(
    listing_id=pw.left.listing_id,
    title=pw.left.title,
    price=pw.left.price,
    location=pw.left.location,
    source=pw.left.source,
    posted_date=pw.left.posted_date,
    agent_name=pw.coalesce(pw.right.full_name, "Unknown"),
)

# ── WRITE to PostgreSQL ──────────────────────────────────────────────────────

pw.io.postgres.write(
    enriched_employees,
    postgres_settings,
    "enriched_employees",
    output_table_type="snapshot",
    primary_key=[enriched_employees.employee_id],
)

pw.io.postgres.write(
    enriched_listings,
    postgres_settings,
    "enriched_listings",
    output_table_type="snapshot",
    primary_key=[enriched_listings.listing_id],
)

# Wait for RabbitMQ streams to be created by the producer
time.sleep(15)

# Launch the streaming computation
pw.run()

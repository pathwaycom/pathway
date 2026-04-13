CREATE TABLE IF NOT EXISTS enriched_employees (
    employee_id INTEGER PRIMARY KEY,
    full_name TEXT NOT NULL,
    email TEXT NOT NULL,
    phone TEXT NOT NULL,
    department TEXT NOT NULL,
    salary DOUBLE PRECISION NOT NULL,
    hire_date TEXT NOT NULL,
    is_active BOOLEAN NOT NULL,
    total_orders BIGINT NOT NULL,
    total_spent DOUBLE PRECISION NOT NULL,
    total_listings BIGINT NOT NULL,
    avg_listing_price DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS enriched_listings (
    listing_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    location TEXT NOT NULL,
    source TEXT NOT NULL,
    posted_date TEXT NOT NULL,
    agent_name TEXT NOT NULL
);

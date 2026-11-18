CREATE TABLE IF NOT EXISTS quarterly_earnings (
    company_symbol TEXT NOT NULL,
    eps FLOAT NOT NULL,
    net_income_md FLOAT NOT NULL,
    quarter TEXT NOT NULL,
    revenue_md FLOAT NOT NULL,
    year BIGINT NOT NULL,
    time BIGINT NOT NULL,
    diff INTEGER NOT NULL
);
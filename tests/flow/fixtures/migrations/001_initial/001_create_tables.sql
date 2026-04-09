-- Migration 001: Create initial tables

CREATE TABLE IF NOT EXISTS order_sync (
    cdc_lsn TEXT,
    cdc_transaction_id TEXT,
    cdc_source_table TEXT,
    cdc_operation INTEGER,
    order_id INTEGER PRIMARY KEY,
    amount REAL,
    status TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS order_item_sync (
    cdc_lsn TEXT,
    cdc_transaction_id TEXT,
    cdc_source_table TEXT,
    cdc_operation INTEGER,
    order_item_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    item_name TEXT,
    price REAL
);

CREATE TABLE IF NOT EXISTS inventory_sync (
    cdc_lsn TEXT,
    cdc_transaction_id TEXT,
    cdc_source_table TEXT,
    cdc_operation INTEGER,
    product_id INTEGER PRIMARY KEY,
    quantity INTEGER
);

CREATE TABLE IF NOT EXISTS enriched_orders (
    cdc_lsn TEXT,
    cdc_transaction_id TEXT,
    cdc_source_table TEXT,
    cdc_operation INTEGER,
    order_id INTEGER PRIMARY KEY,
    amount REAL,
    status TEXT,
    created_at TEXT
);

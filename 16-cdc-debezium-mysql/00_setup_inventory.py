#!/usr/bin/env python3
"""Create the `inventory` database used by all CDC examples (idempotent: drops & recreates it).

    python 16-cdc-debezium-mysql/00_setup_inventory.py

Also prints the MySQL settings Debezium depends on (binlog format/row image, GTID, the debezium user's grants).
"""
from cdc_common import mysql

DDL = """
DROP DATABASE IF EXISTS inventory;
CREATE DATABASE inventory;
USE inventory;
CREATE TABLE customers (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(50)  NOT NULL,
  last_name  VARCHAR(50)  NOT NULL,
  email      VARCHAR(100) NOT NULL UNIQUE,
  city       VARCHAR(50)
);
CREATE TABLE products (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(100) NOT NULL,
  category    VARCHAR(50)  NOT NULL,
  price       DECIMAL(10,2) NOT NULL,
  stock       INT NOT NULL DEFAULT 0
);
CREATE TABLE orders (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT NOT NULL,
  product_id  INT NOT NULL,
  quantity    INT NOT NULL,
  status      VARCHAR(20) NOT NULL DEFAULT 'NEW',
  created_at  TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);
-- Transactional outbox (07_outbox_pattern.py): written in the SAME transaction as the business change
CREATE TABLE outbox (
  id            CHAR(36)     NOT NULL PRIMARY KEY,
  aggregatetype VARCHAR(255) NOT NULL,      -- routes to topic outbox.event.<aggregatetype>
  aggregateid   VARCHAR(255) NOT NULL,      -- becomes the Kafka KEY (ordering per aggregate)
  type          VARCHAR(255) NOT NULL,      -- event type, copied to a header
  payload       JSON         NOT NULL
);
INSERT INTO customers (first_name, last_name, email, city) VALUES
  ('Sally', 'Thomas', 'sally.thomas@acme.com', 'Austin'),
  ('George', 'Bailey', 'gbailey@foobar.com', 'Bedford Falls'),
  ('Edward', 'Walker', 'ed@walker.com', 'Boston'),
  ('Anne', 'Kretchmar', 'annek@noanswer.org', 'Denver');
INSERT INTO products (name, category, price, stock) VALUES
  ('scooter', 'vehicles', 3.14, 10), ('car battery', 'auto', 8.10, 5),
  ('12-pack drill bits', 'tools', 0.80, 100), ('hammer', 'tools', 2.75, 40),
  ('rocks', 'garden', 5.30, 7), ('spare tire', 'auto', 22.20, 3);
INSERT INTO orders (customer_id, product_id, quantity) VALUES (1, 1, 1), (2, 3, 2), (1, 4, 1);
GRANT ALL PRIVILEGES ON inventory.* TO 'app'@'%';
"""

db = mysql(None)
with db.cursor() as cur:
    for stmt in [s.strip() for s in DDL.split(";") if s.strip()]:
        cur.execute(stmt)
    print("created database inventory: customers(4) products(6) orders(3) outbox(0)\n")
    print("MySQL settings Debezium relies on:")
    for var in ("log_bin", "binlog_format", "binlog_row_image", "gtid_mode", "binlog_expire_logs_seconds", "server_id"):
        cur.execute("SHOW VARIABLES LIKE %s", (var,))
        r = cur.fetchone()
        print(f"  {r['Variable_name']:28s} = {r['Value']}")
    cur.execute("SHOW GRANTS FOR 'debezium'@'%'")
    print("\n  " + "\n  ".join(list(r.values())[0] for r in cur.fetchall()))
    cur.execute("SHOW MASTER STATUS")
    r = cur.fetchone()
    print(f"\ncurrent binlog position: file={r['File']} pos={r['Position']}")

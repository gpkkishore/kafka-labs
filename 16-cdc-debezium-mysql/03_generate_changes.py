#!/usr/bin/env python3
"""Generate realistic INSERT / UPDATE / DELETE traffic on the inventory database (each loop = 1 transaction).

    python 16-cdc-debezium-mysql/03_generate_changes.py                  # 20 transactions
    python 16-cdc-debezium-mysql/03_generate_changes.py -n 0 --rate 5    # forever, 5 tx/s (load for Grafana/Flink)
"""
import argparse
import random
import time

from cdc_common import mysql
from faker import Faker

ap = argparse.ArgumentParser()
ap.add_argument("-n", type=int, default=20, help="number of transactions (0 = forever)")
ap.add_argument("--rate", type=float, default=2.0, help="transactions per second")
args = ap.parse_args()
fake = Faker()
db = mysql()
db.autocommit(False)
i = 0
try:
    while args.n == 0 or i < args.n:
        i += 1
        with db.cursor() as cur:
            action = random.choices(["new_customer", "update_customer", "delete_customer", "price_change",
                                     "new_order", "order_status", "multi_row"], [2, 3, 1, 3, 4, 3, 1])[0]
            if action == "new_customer":
                cur.execute("INSERT INTO customers (first_name,last_name,email,city) VALUES (%s,%s,%s,%s)",
                            (fake.first_name(), fake.last_name(), fake.unique.email(), fake.city()))
                msg = f"INSERT customer id={cur.lastrowid}"
            elif action == "update_customer":
                cur.execute("UPDATE customers SET city=%s ORDER BY RAND() LIMIT 1", (fake.city(),))
                msg = "UPDATE customer city"
            elif action == "delete_customer":
                cur.execute("SELECT id FROM customers WHERE id > 4 ORDER BY RAND() LIMIT 1")   # keep the 4 seed rows
                r = cur.fetchone()
                if r:
                    cur.execute("DELETE FROM customers WHERE id=%s", (r["id"],))
                msg = f"DELETE customer {r['id'] if r else '(none)'}"
            elif action == "price_change":
                cur.execute("UPDATE products SET price=ROUND(price*%s,2) ORDER BY RAND() LIMIT 1", (random.uniform(0.8, 1.2),))
                msg = "UPDATE product price"
            elif action == "new_order":
                cur.execute("INSERT INTO orders (customer_id,product_id,quantity) "
                            "SELECT (SELECT id FROM customers ORDER BY RAND() LIMIT 1), (SELECT id FROM products ORDER BY RAND() LIMIT 1), %s",
                            (random.randint(1, 5),))
                cur.execute("UPDATE products SET stock=GREATEST(stock-1,0) ORDER BY RAND() LIMIT 1")
                msg = "INSERT order + UPDATE stock (1 tx, 2 tables)"
            elif action == "order_status":
                cur.execute("UPDATE orders SET status=%s WHERE status <> 'SHIPPED' ORDER BY RAND() LIMIT 1",
                            (random.choice(["PAID", "SHIPPED"]),))
                msg = "UPDATE order status"
            else:
                cur.execute("UPDATE products SET stock=stock+10 WHERE category='tools'")
                msg = f"UPDATE {cur.rowcount} tool products in ONE statement -> {cur.rowcount} change events"
        db.commit()
        print(f"tx {i:4d}: {msg}")
        time.sleep(1 / args.rate)
except KeyboardInterrupt:
    pass

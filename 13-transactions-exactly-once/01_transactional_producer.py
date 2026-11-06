#!/usr/bin/env python3
"""KiA 2.6.2 / AKiA 5.2 — a transactional producer: several writes become visible atomically, or not at all.

    python 13-transactions-exactly-once/01_transactional_producer.py

Transaction 1: 3 payments -> COMMIT
Transaction 2: 3 payments -> ABORT   (they ARE written to the log, but marked aborted)
Transaction 3: writes to TWO topics atomically -> COMMIT
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, log, recreate_topic  # noqa: E402

PAYMENTS, LEDGER = "tx_payments", "tx_ledger"
recreate_topic(PAYMENTS, 1, 3)
recreate_topic(LEDGER, 1, 3)

p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "transactional.id": "payments-service-1",   # stable id per producer INSTANCE (survives restarts)
    "enable.idempotence": True,                 # implied by transactional.id
})
p.init_transactions(10)    # registers with the transaction coordinator, fences older instances, gets PID+epoch
log("init_transactions() done")

banner("TX 1 -> commit")
p.begin_transaction()
for i in range(3):
    p.produce(PAYMENTS, key=f"pay-{i}", value=f"payment {i} (committed tx)")
p.commit_transaction()
log("committed")

banner("TX 2 -> abort")
p.begin_transaction()
for i in range(3, 6):
    p.produce(PAYMENTS, key=f"pay-{i}", value=f"payment {i} (ABORTED tx)")
p.flush()                  # records really reach the broker log...
p.abort_transaction()      # ...but get an ABORT marker
log("aborted")

banner("TX 3 -> two topics, one atomic commit")
p.begin_transaction()
p.produce(PAYMENTS, key="pay-6", value="payment 6 (committed tx, with ledger entry)")
p.produce(LEDGER, key="acct-42", value="debit 6")
p.commit_transaction()
log("committed")
print("""
Now run 02_read_committed_vs_uncommitted.py to see who sees what,
and 05_dump_control_records.sh to see the COMMIT/ABORT markers inside the log segment.
""")

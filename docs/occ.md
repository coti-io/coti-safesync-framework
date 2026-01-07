# Optimistic Concurrency Control (OCC) in COTI SafeSync Framework

## Overview

Optimistic Concurrency Control (OCC) is a concurrency strategy that **detects conflicts instead of preventing them with locks**.

COTI SafeSync Framework provides a low-level helper, `occ_update`, that implements the **atomic SQL primitive** required for OCC.
However, **correctness depends on how the helper is used**, not just on the SQL statement itself.

This document defines the **mandatory usage contract**, explains **why it exists**, and provides **correct and incorrect examples**.

---

## What OCC Guarantees (and What It Does Not)

OCC guarantees:

* No lost updates
* Each successful update is based on a unique, consistent version
* Writers never overwrite each other silently

OCC does **not** guarantee:

* Immediate success (retries are expected)
* Fairness between competing writers
* Absence of database contention

Retries are part of the design.

---

## Data Model Requirements

To use OCC, your table **must** include a version column.

Example schema:

```sql
CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    amount INT NOT NULL,
    version INT NOT NULL
) ENGINE=InnoDB;
```

Rules:

* `version` must be incremented on every successful update
* Reads must fetch both the business data and the version

---

## The OCC Primitive Provided by COTI SafeSync Framework

COTI SafeSync Framework exposes the following helper:

```python
occ_update(
    session: DbSession,
    table: str,
    id_column: str,
    id_value: Any,
    version_column: str,
    version_value: Any,
    updates: Mapping[str, Any],
) -> int
```

It executes SQL of the form:

```sql
UPDATE table
SET <updates>, version = version + 1
WHERE id = :id AND version = :expected_version
```

### Return Value

* `1` → update succeeded, version matched
* `0` → update failed (version mismatch, missing row, or no-op)

**A return value of `0` is not an error. It is a concurrency signal.**

---

## Mandatory OCC Usage Contract (Read This Carefully)

OCC correctness **requires** all of the following:

### 1. Each OCC attempt MUST run in its own transaction

* A single read → conditional update → commit
* Never retry inside the same `DbSession`

### 2. `rowcount == 0` MUST trigger a retry

* Ignoring it causes silent data loss

### 3. Retries MUST re-read the row

* You must obtain the new version before retrying

### 4. Transactions MUST be short-lived

* Holding a transaction across retries causes lock contention and timeouts

### 5. Retryable MySQL errors MUST be retried

Specifically:

* `1205` — lock wait timeout
* `1213` — deadlock detected

Any other exception must fail fast.

### 6. A retry limit MUST exist

Infinite retries are forbidden.

---

## ✅ Correct Usage (Reference Pattern)

This is the **canonical OCC pattern** in COTI SafeSync Framework.

Use this as your reference.

```python
MAX_RETRIES = 100

def update_order_amount(engine, order_id: int, delta: int) -> None:
    for attempt in range(MAX_RETRIES):
        try:
            with DbSession(engine) as session:
                row = session.fetch_one(
                    "SELECT amount, version FROM orders WHERE id = :id",
                    {"id": order_id},
                )
                if row is None:
                    return  # row does not exist

                rc = occ_update(
                    session=session,
                    table="orders",
                    id_column="id",
                    id_value=order_id,
                    version_column="version",
                    version_value=row["version"],
                    updates={"amount": row["amount"] + delta},
                )

                if rc == 1:
                    return  # success (commit happens on exit)

            # rc == 0 → version mismatch → retry with a new transaction

        except OperationalError as e:
            if is_retryable_mysql_error(e):
                time.sleep(random.uniform(0, 0.005))
                continue
            raise

    raise RuntimeError("OCC retry limit exceeded")
```

### Why this works

* Each retry uses a **fresh transaction**
* Conflicts are detected, not blocked
* Locks (if any) are held for minimal time
* MySQL deadlocks/timeouts are handled correctly

---

## ❌ Incorrect Usage (Common Pitfall)

### Retrying inside a single transaction (DO NOT DO THIS)

```python
# ❌ Incorrect
with DbSession(engine) as session:
    while True:
        row = session.fetch_one(...)
        rc = occ_update(...)
        if rc == 1:
            break
```

### Why this is wrong

* Holds row locks across retries
* Causes lock wait timeouts (1205)
* Can deadlock under contention
* Violates OCC semantics

This pattern **will fail under load**, even though it may appear to work in light testing.

---

## OCC vs Locks

OCC is **not a replacement** for locks in all cases.

| Pattern      | Use When                                   |
| ------------ | ------------------------------------------ |
| OCC          | Conflicts are rare, retries acceptable     |
| RowLock      | Read-modify-write must be serialized       |
| AdvisoryLock | You need a cross-row or logical mutex      |
| Atomic SQL   | You can express the update without reading |

OCC works best when combined with **short transactions and retry logic**, not with long-held locks.

---

## Relationship to Concurrency Tests

OCC correctness is validated by:

* `tests/concurrency/test_invariant_occ.py::test_invariant_occ_correctness_with_retry`

Demonstration of incorrect usage:

* `tests/concurrency/test_invariant_occ_demo_without_retry`

The demo test intentionally violates the OCC contract to show failure modes.

---

## Summary

* `occ_update` is a **primitive**, not a full algorithm
* Correctness depends on **transaction boundaries and retries**
* Retry logic is mandatory
* Short transactions are essential
* Tests validate the contract; this document explains it

If you follow the patterns in this document, OCC in COTI SafeSync Framework is **safe under high concurrency**.

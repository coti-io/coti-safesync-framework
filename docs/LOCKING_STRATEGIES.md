# Locking and Synchronization Strategies

This document explains all concurrency control strategies available in `conquiet` for safe concurrent database operations.

## Table of Contents

1. [Pessimistic Row Lock](#1-pessimistic-row-lock)
2. [Optimistic Concurrency Control (OCC)](#2-optimistic-concurrency-control-occ)
3. [Advisory Lock](#3-advisory-lock)
4. [Advisory + Row Lock](#4-advisory--row-lock)
5. [Atomic SQL Updates](#5-atomic-sql-updates)
6. [Idempotent INSERT](#6-idempotent-insert)

---

## 1. Pessimistic Row Lock

### Overview

**Pessimistic row locking** uses `SELECT ... FOR UPDATE` to acquire an exclusive lock on specific rows before reading them. The lock is held until the transaction commits or rolls back.

### Characteristics

- **Lock scope**: Transaction-scoped (released on commit/rollback)
- **Lock type**: Exclusive row-level lock
- **Contention**: High (serializes access to locked rows)
- **Performance**: Lower throughput, but guarantees no conflicts
- **Use when**: You need strict serialization and can tolerate lock contention

### Use Case

**Scenario**: Processing orders sequentially to prevent double-processing

You have multiple worker processes that need to claim and process orders. Only one worker should process a specific order at a time.

### Example

```python
from conquiet.db.session import DbSession
from conquiet.db.locking.row_lock import RowLock

def process_order(engine, order_id: int) -> None:
    """
    Process an order using pessimistic row locking.
    Only one worker can process a specific order at a time.
    """
    with DbSession(engine) as session:
        # Acquire exclusive lock on the order row
        order = RowLock(session, "orders", {"id": order_id}).acquire()
        
        if order is None:
            # Order doesn't exist
            return
        
        # Check if already processed
        if order["status"] == "processed":
            return  # Already processed by another worker
        
        # Process the order (read-modify-write)
        session.execute(
            "UPDATE orders SET status = :status, processed_at = NOW() WHERE id = :id",
            {"id": order_id, "status": "processed"}
        )
        # Lock is released when transaction commits
```

### Important Notes

- ⚠️ **Do NOT use RowLock inside retry loops** - each lock acquisition should guard exactly one operation
- ⚠️ **Index your WHERE clause columns** - non-indexed predicates can cause full table scans and gap locks
- ⚠️ **Keep transactions short** - long-held locks increase contention and deadlock risk
- The lock is automatically released when the transaction commits or rolls back

---

## 2. Optimistic Concurrency Control (OCC)

### Overview

**Optimistic Concurrency Control** uses version columns to detect conflicts. Instead of locking, it checks if the version matches before updating. If the version changed (conflict detected), the update fails and must be retried.

### Characteristics

- **Lock scope**: No locks (optimistic)
- **Conflict detection**: Version column comparison
- **Contention**: Low (allows concurrent reads)
- **Performance**: High throughput under low contention
- **Use when**: Contention is low, or you can tolerate retries

### Use Case

**Scenario**: Updating account balances with high read-to-write ratio

Multiple processes read account balances frequently, but updates are less common. You want high throughput for reads and can handle occasional retries on writes.

### Example

```python
from conquiet.db.session import DbSession
from conquiet.db.helpers import occ_update
import random
import time

def update_account_balance(engine, account_id: int, amount_change: int) -> None:
    """
    Update account balance using OCC with retry.
    """
    MAX_RETRIES = 10
    
    for attempt in range(MAX_RETRIES):
        with DbSession(engine) as session:
            # Read current balance and version
            account = session.fetch_one(
                "SELECT id, balance, version FROM accounts WHERE id = :id",
                {"id": account_id}
            )
            
            if account is None:
                raise ValueError(f"Account {account_id} not found")
            
            current_balance = int(account["balance"])
            current_version = int(account["version"])
            new_balance = current_balance + amount_change
            
            # Attempt OCC update
            rowcount = occ_update(
                session=session,
                table="accounts",
                id_column="id",
                id_value=account_id,
                version_column="version",
                version_value=current_version,
                updates={"balance": new_balance}
            )
            
            if rowcount == 1:
                # Success! Transaction commits automatically
                return
        
        # Version mismatch - retry with a new transaction
        # Small randomized backoff to reduce contention
        time.sleep(random.uniform(0.001, 0.01))
    
    raise RuntimeError(f"Failed to update account {account_id} after {MAX_RETRIES} retries")
```

### Important Notes

- ⚠️ **MUST retry on `rowcount == 0`** - this indicates a version mismatch
- ⚠️ **MUST re-read before retrying** - don't reuse stale data
- ⚠️ **Each retry uses a NEW transaction** - retry loop must be outside `DbSession`
- ⚠️ **Keep transactions short** - no retry loops inside a single session
- The `occ_update()` helper automatically increments the version column

---

## 3. Advisory Lock

### Overview

**Advisory locks** are application-level mutexes using MySQL's `GET_LOCK()`. They are connection-scoped (not transaction-scoped) and provide general-purpose synchronization.

### Characteristics

- **Lock scope**: Connection-scoped (released when connection closes)
- **Lock type**: Named mutex (application-level)
- **Contention**: Medium (serializes access to named resources)
- **Performance**: Good for coarse-grained synchronization
- **Use when**: You need to synchronize operations across multiple tables or non-row resources

### Use Case

**Scenario**: Preventing concurrent processing of the same user's data

Multiple workers process user data across multiple tables. You want to ensure only one worker processes a specific user's data at a time, even if operations span multiple tables.

### Example

```python
from conquiet.db.session import DbSession
from conquiet.db.locking.advisory_lock import AdvisoryLock
from conquiet.errors import LockTimeoutError

def process_user_data(engine, user_id: int) -> None:
    """
    Process all data for a user using advisory lock.
    Ensures only one worker processes this user's data at a time.
    """
    lock_key = f"user_processing:{user_id}"
    
    try:
        with DbSession(engine) as session:
            with AdvisoryLock(session, lock_key, timeout=10):
                # Lock acquired - we're the only worker processing this user
                
                # Read user's orders
                orders = session.fetch_all(
                    "SELECT id, total FROM orders WHERE user_id = :user_id",
                    {"user_id": user_id}
                )
                
                # Update user's summary
                total_spent = sum(order["total"] for order in orders)
                session.execute(
                    "UPDATE users SET total_spent = :total WHERE id = :id",
                    {"id": user_id, "total": total_spent}
                )
                
                # Process orders (could involve multiple tables)
                for order in orders:
                    session.execute(
                        "UPDATE orders SET processed = 1 WHERE id = :id",
                        {"id": order["id"]}
                    )
                
                # Lock is released when DbSession closes (after commit)
                
    except LockTimeoutError:
        # Another worker is processing this user - skip or retry later
        print(f"Could not acquire lock for user {user_id} - skipping")
```

### Important Notes

- ⚠️ **Lock is held across transaction commit** - this prevents other connections from observing stale state
- ⚠️ **Do NOT use inside retry loops** - each lock acquisition should guard one logical operation
- ⚠️ **Do NOT use to "protect" OCC** - OCC has its own conflict detection
- Lock is released when the `DbSession` connection closes (after commit/rollback)
- Lock keys should be unique and meaningful (e.g., `"order:42"`, `"user:123"`)

---

## 4. Advisory + Row Lock

### Overview

**Combining Advisory and Row locks** provides both coarse-grained (advisory) and fine-grained (row) synchronization. This pattern is useful when you need to protect both application-level resources and specific database rows.

### Characteristics

- **Lock scope**: Both connection-scoped (advisory) and transaction-scoped (row)
- **Lock type**: Named mutex + exclusive row lock
- **Contention**: High (double serialization)
- **Performance**: Lower throughput, maximum safety
- **Use when**: You need both application-level and row-level protection

### Use Case

**Scenario**: Processing orders with inventory checks

You need to ensure only one worker processes orders for a specific customer (advisory lock) AND prevent concurrent modifications to inventory items (row lock).

### Example

```python
from conquiet.db.session import DbSession
from conquiet.db.locking.advisory_lock import AdvisoryLock
from conquiet.db.locking.row_lock import RowLock
from conquiet.errors import LockTimeoutError

def process_order_with_inventory(engine, order_id: int, customer_id: int) -> None:
    """
    Process an order using both advisory and row locks.
    Advisory lock: ensures only one worker processes this customer's orders
    Row lock: ensures inventory items aren't modified concurrently
    """
    advisory_key = f"customer_orders:{customer_id}"
    
    try:
        with DbSession(engine) as session:
            # Coarse-grained: lock customer's order processing
            with AdvisoryLock(session, advisory_key, timeout=10):
                # Fine-grained: lock the specific order row
                order = RowLock(session, "orders", {"id": order_id}).acquire()
                
                if order is None:
                    return  # Order doesn't exist
                
                if order["status"] != "pending":
                    return  # Already processed
                
                # Lock inventory items
                items = session.fetch_all(
                    "SELECT product_id, quantity FROM order_items WHERE order_id = :order_id",
                    {"order_id": order_id}
                )
                
                for item in items:
                    # Lock each inventory row
                    inventory = RowLock(
                        session, 
                        "inventory", 
                        {"product_id": item["product_id"]}
                    ).acquire()
                    
                    if inventory is None:
                        raise ValueError(f"Product {item['product_id']} not found")
                    
                    if inventory["stock"] < item["quantity"]:
                        raise ValueError("Insufficient stock")
                    
                    # Update inventory
                    session.execute(
                        "UPDATE inventory SET stock = stock - :qty WHERE product_id = :pid",
                        {"qty": item["quantity"], "pid": item["product_id"]}
                    )
                
                # Mark order as processed
                session.execute(
                    "UPDATE orders SET status = :status WHERE id = :id",
                    {"id": order_id, "status": "processed"}
                )
                
    except LockTimeoutError:
        # Another worker is processing this customer's orders
        print(f"Could not acquire lock for customer {customer_id} - skipping")
```

### Important Notes

- ⚠️ **Use sparingly** - double locking increases contention significantly
- ⚠️ **Acquire locks in consistent order** - advisory first, then row locks, to prevent deadlocks
- ⚠️ **Keep transactions short** - both locks are held until commit
- This pattern provides maximum safety but lowest throughput

---

## 5. Atomic SQL Updates

### Overview

**Atomic SQL updates** leverage MySQL's built-in atomicity guarantees. Single-statement updates are automatically atomic and don't require explicit locks.

### Characteristics

- **Lock scope**: None (database handles it internally)
- **Lock type**: Implicit row-level locks (brief, during statement execution)
- **Contention**: Low (locks held only during statement execution)
- **Performance**: Highest throughput
- **Use when**: Your operation can be expressed as a single atomic SQL statement

### Use Case

**Scenario**: Incrementing counters or updating simple aggregates

Multiple workers need to increment counters or update simple values. The operation can be expressed as a single SQL statement.

### Example

```python
from conquiet.db.session import DbSession

def increment_page_views(engine, page_id: int) -> None:
    """
    Increment page view counter using atomic SQL update.
    No explicit locks needed - MySQL handles atomicity.
    """
    with DbSession(engine) as session:
        # Single atomic statement - no locks needed
        session.execute(
            "UPDATE pages SET view_count = view_count + 1 WHERE id = :id",
            {"id": page_id}
        )
        # Transaction commits automatically

def update_user_score(engine, user_id: int, points: int) -> None:
    """
    Update user score using atomic SQL arithmetic.
    """
    with DbSession(engine) as session:
        session.execute(
            "UPDATE users SET score = score + :points, last_updated = NOW() WHERE id = :id",
            {"id": user_id, "points": points}
        )
```

### Important Notes

- ✅ **No explicit locks needed** - MySQL guarantees atomicity for single statements
- ✅ **Highest performance** - minimal overhead
- ⚠️ **Limited to single-statement operations** - complex logic may require other strategies
- Works for: counters, simple arithmetic, timestamp updates, flag toggles

---

## 6. Idempotent INSERT

### Overview

**Idempotent INSERT** relies on database constraints (PRIMARY KEY, UNIQUE) to prevent duplicate inserts. Duplicate key errors are caught and handled gracefully.

### Characteristics

- **Lock scope**: None (database constraint enforcement)
- **Lock type**: Implicit constraint checks
- **Contention**: Low (only conflicts on duplicate keys)
- **Performance**: High (no explicit locking overhead)
- **Use when**: You need to ensure at most one row exists for a given key

### Use Case

**Scenario**: Creating records that should only exist once

Multiple workers attempt to create the same record (e.g., initializing user profiles, creating default settings). You want to ensure only one record is created, even if multiple workers try simultaneously.

### Example

```python
from conquiet.db.session import DbSession
from sqlalchemy.exc import IntegrityError

def create_user_profile(engine, user_id: int, initial_data: dict) -> None:
    """
    Create user profile using idempotent INSERT.
    If profile already exists, this is a no-op.
    """
    with DbSession(engine) as session:
        try:
            session.execute(
                """
                INSERT INTO user_profiles (user_id, display_name, created_at)
                VALUES (:user_id, :display_name, NOW())
                """,
                {
                    "user_id": user_id,
                    "display_name": initial_data.get("display_name", "User")
                }
            )
            # Success - profile created
        except IntegrityError:
            # Profile already exists (duplicate key) - this is fine
            # The operation is idempotent - we can safely ignore the error
            pass

def initialize_user_settings(engine, user_id: int) -> None:
    """
    Initialize default settings for a user.
    Multiple workers may try to initialize - only one succeeds.
    """
    with DbSession(engine) as session:
        try:
            session.execute(
                """
                INSERT INTO user_settings (user_id, theme, notifications_enabled)
                VALUES (:user_id, 'default', 1)
                """,
                {"user_id": user_id}
            )
        except IntegrityError:
            # Settings already initialized - no action needed
            pass
```

### Important Notes

- ✅ **No explicit locks needed** - database constraints handle uniqueness
- ✅ **Idempotent by design** - safe to retry
- ⚠️ **Must handle `IntegrityError`** - catch duplicate key errors gracefully
- ⚠️ **Requires PRIMARY KEY or UNIQUE constraint** - won't work without constraints
- Works for: initialization, one-time setup, idempotent record creation

---

## Strategy Comparison

| Strategy | Lock Type | Contention | Performance | Use When |
|----------|-----------|------------|-------------|----------|
| **Row Lock** | Pessimistic | High | Low-Medium | Need strict serialization |
| **OCC** | Optimistic | Low | High | Low contention, can retry |
| **Advisory Lock** | Application mutex | Medium | Medium | Coarse-grained sync |
| **Advisory + Row** | Both | Very High | Low | Maximum safety needed |
| **Atomic SQL** | None (implicit) | Low | Highest | Single-statement ops |
| **Idempotent INSERT** | None (constraints) | Low | High | One-time initialization |

## Decision Guide

**Choose Row Lock when:**
- You need strict serialization
- Contention is acceptable
- You can't tolerate retries

**Choose OCC when:**
- Contention is low
- You can handle retries
- You want high throughput

**Choose Advisory Lock when:**
- You need to synchronize across multiple tables
- You're protecting application-level resources
- Row-level locking isn't sufficient

**Choose Advisory + Row Lock when:**
- You need maximum safety
- You're protecting both application and database resources
- Performance is less critical

**Choose Atomic SQL when:**
- Your operation is a single SQL statement
- You want maximum performance
- No complex logic needed

**Choose Idempotent INSERT when:**
- You're creating records that should exist once
- You can handle duplicate key errors
- You want initialization to be safe to retry

---

## Additional Resources

- [bootstrap.md](./bootstrap.md) - Complete API documentation
- [bootstrap_concurrency_tests.md](./bootstrap_concurrency_tests.md) - Concurrency test specifications
- [occ.md](./occ.md) - Detailed OCC usage guide


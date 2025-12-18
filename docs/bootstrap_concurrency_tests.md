# `bootstrap_concurrency_tests.md`

## 1. Purpose

This document defines how **conquiet’s database concurrency primitives** are validated and proven production-ready.

Correctness is defined **not** by absence of errors or timing behavior, but by preservation of **invariants** under concurrent execution across multiple processes.

The goal of these tests is to demonstrate that:

* Unsafe concurrency patterns *can* break correctness
* The synchronization mechanisms provided by conquiet *prevent* such breakage
* All guarantees stated in `bootstrap.md` are upheld under load comparable to (or exceeding) production conditions

---

## 2. Testing philosophy

### 2.1 Invariants over races

We do **not** attempt to “observe races” directly.

Concurrency bugs are inherently non-deterministic and timing-dependent.
Instead, we assert **invariants**: properties of the system that must hold regardless of interleaving or scheduling.

A test passes if the invariant holds.
A test fails if the invariant is violated.

---

### 2.2 Two classes of tests

All concurrency tests fall into one of two categories:

#### A. Correctness tests (positive tests)

These assert that an invariant **must always hold** when using a given synchronization mechanism.

* If the invariant fails even once → **test failure**
* These tests must **not** be retried
* Any failure indicates a correctness bug in conquiet

#### B. Demonstration tests (negative tests)

These demonstrate that an invariant is **not guaranteed** when synchronization is absent or misused.

* These tests are inherently probabilistic
* They may be executed multiple times
* A test is considered successful if **at least one execution violates the invariant**
* If the invariant never fails across retries, the test fails (unsafety was not demonstrated)

---

## 3. Execution model

All concurrency tests must adhere to the following execution model:

* Real MySQL database (InnoDB engine)
* Multiple OS processes (`multiprocessing`), not threads
* Each process uses its own `DbSession`
* Workers synchronize start using a barrier to maximize contention
* No mocking of database behavior
* No reliance on sleeps or timing assumptions

---

## 4. Load profile

Default test parameters (may be adjusted upward if tests are too stable):

* Processes: 5–10
* Operations per process: 50–100
* Total operations: 250–1000
* All workers operate on the **same row(s)** to maximize contention

The objective is to create realistic or worst-case contention, not minimal examples.

---

## 5. Core invariants

### 5.1 Invariant: No lost updates (read-modify-write)

**Scenario**

Multiple workers concurrently perform a read-modify-write cycle on the same row.

**Invariant**

> Final value must equal
> `initial_value + (processes × updates_per_process)`

**Expected outcomes**

| Synchronization | Expected                    |
| --------------- | --------------------------- |
| None            | ❌ Invariant may be violated |
| RowLock         | ✅ Invariant must hold       |
| AdvisoryLock    | ✅ Invariant must hold       |
| Advisory + Row  | ✅ Invariant must hold       |

---

### 5.2 Invariant: Atomic SQL correctness

**Scenario**

Multiple workers perform:

```sql
UPDATE counters SET value = value + 1 WHERE id = 1;
```

**Invariant**

> Final value equals number of UPDATE statements executed

**Expected outcomes**

| Synchronization | Expected              |
| --------------- | --------------------- |
| None            | ✅ Invariant must hold |

This validates that atomic SQL updates do not require explicit locks.

---

### 5.3 Invariant: Optimistic Concurrency Control (OCC)

**Scenario**

Multiple workers attempt OCC updates using a version column.

**Invariant**

> Each successful update consumes a unique version value
> No two successful updates may use the same version

**Expected outcomes**

| Handling                 | Expected                    |
| ------------------------ | --------------------------- |
| Ignore rowcount          | ❌ Invariant may be violated |
| Retry on `rowcount == 0` | ✅ Invariant must hold       |

OCC does not prevent conflicts; it detects them.
Correct handling is the caller’s responsibility.

---

### 5.4 Invariant: Idempotent INSERT

**Scenario**

Multiple workers concurrently attempt to insert the same primary key.

**Invariant**

> At most one row exists for the given primary key

**Expected outcomes**

| Synchronization | Expected              |
| --------------- | --------------------- |
| None            | ✅ Invariant must hold |

This validates that duplicate key handling is sufficient without locks.

---

## 6. Test structure guidelines

* Each invariant should have:

  * One negative (demonstration) test, if applicable
  * One or more positive (correctness) tests
* Tests must assert **only final observable database state**
* Tests must not assert on:

  * logs
  * timing
  * ordering
  * internal lock state

---

## 7. Failure handling (critical)

### 7.1 Correctness tests

If a correctness test violates its invariant:

* The test **fails immediately**
* The test is **not retried**
* This indicates a bug in conquiet

### 7.2 Demonstration tests

Because unsafety is probabilistic:

* The test may be run multiple times
* The test passes if **any run violates the invariant**
* The test fails if **all runs preserve the invariant**

---

## 8. CI considerations

* Concurrency tests are expected to be slower than unit tests
* They may be grouped or marked separately (e.g., `@pytest.mark.concurrency`)
* CI must use real MySQL with InnoDB enabled
* Tests should run deterministically under CI load

---

## 9. What these tests prove

If all correctness tests pass under concurrent load:

* DbSession transaction semantics are correct
* AdvisoryLock and RowLock provide the stated guarantees
* OCC helpers behave as documented
* No lost updates or silent overwrites occur under supported usage

This constitutes evidence that conquiet’s DB primitives are safe for production use.

---

## 10. What these tests do NOT prove

These tests do not guarantee:

* absence of contention
* absence of retries
* absence of deadlocks under arbitrary misuse

They guarantee **correctness when APIs are used as documented**.

---

## 11. Summary

> Conquiet’s concurrency guarantees are validated by asserting invariants under real multi-process load, not by observing timing behavior or race symptoms.

---

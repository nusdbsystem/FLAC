## MockKV

A simple in-memory key-value store used for experiment.

- Transaction supported: Serializable.
- Timeout for locks supported. (-1 for keep waiting)
- WAL supported for updates. (-1 for stable log, txnID for temporary logs)

You can just use MockKV with the following APIs.

#### APIs

| Exposed function |                         Explanation                          | Thread-safe |
| ---------------- | :----------------------------------------------------------: | :---------: |
| NewKV            | Create a new KV-store with size len representing the local storage of the shard |      Y      |
| GetDDL/SetDDL    | Get the timeout for locks, Modify the timeout to t for locks. |      Y      |
| Update           |           API for KV-store: storage[key] = value           |      Y      |
| Read             |               API for KV-store: storage[key]               |      Y      |
| Begin            | API for transaction: begin a transaction with given txnID  |      T      |
| ReadTxn          |   Read the value and hold a readCnt lockStat for it with txnID.   |      T      |
| UpdateTxn        | Update the value and hold a write lockStat for it with txnID.  |      T      |
| Commit           |   Release the locks held by txnID, and stable the logs.    |      T      |
| RollBack         |                Abandon the logs, and release                 |      T      |
| Force            | Force to update a value (ignore the timeout, and stabilize the modification) |      Y      |


- Y for thread-safe, T for thread-safe between different transactions.
 
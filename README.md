# FLAC

---

A distributed KV-store with **F**ai**L**ure-**A**ware Atomic **C**ommit protocol supported.

It supports

- 2PC, 3PC, PAC, G-PAC, Easy Commit, and FLAC atomic commit protocols.
- TPC-C and YCSB-like micro-benchmark Test.
- Distributed read-write transactions.

## Build & Test

### Installation

To build the storage
```shell
make build
```
To check if the installation succeeds.
```shell
make check
```

For the local test, please set the `LocalTest` in `config\utils.go` to True, modify `configs\local.json` and `Makefile` according to your IP addresses and ports, and run:

```shell
make local
```

This would add a 10ms delay to specific ports of localhost for the local tests.

If you want to remove the local port delay, run

```shell
make del
```

For the remote test, please update the links in `./configs/remote.json` and `./utils/utils.go`.

If you make changes to FLAC's code, please make sure to rebuild the program and rerun unit tests:

```shell
make build
make test
```

If you make changes to the RL model, rebuild the protocol buffer if needed.

```shell
make buildrpc
```

## Configuration

FLAC configurations can be updated in `configs/glob_var.go`. Here we list several important ones.

```shell
TransactionLength:        number of operations per transaction.
NumberOfShards:           number of partitions for the whole database.
SelectedACP:              atomic commit protocol. Six algorithms are supported (FLAC, 2PC, 3PC, PAC, G-PAC, Easy Commit).
CrossShardTXNPercentage:  the percentage of distributed transactions.
```

## Run
The experiments can be run with
```shell
make exp
```

## Outputs

- `count/cross`: The total number of transactions/distributed transactions.
- `success/crossSuc`: The total number of committed transactions/distributed transactions.
- `p99/P50/ave`: The P99/P50/average latency of all committed transactions.
- `level`: The average robustness level (only for FLAC, 1 for failure-free, 2 for crash failure, 3 for network failure)
- `s1/s2/s3`: the latency breakdown for the first/second/third phase of the commit protocol.
- `error`: The number of transactions that abort due to failures.

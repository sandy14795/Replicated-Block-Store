In this Project, we implemented a Replicated Blob Store system using Primary/Backup replication. A two phase protocol was designed for consistency and log files for crash recovery. For correctness testing, we introduced deterministic crashes in the address space. We also implemented a hashing technique to compare the data for consistency tests. Finally, we evaluated our system on various parameters such as throughput and latency (with or without crash) and measured the recovery time.


# Running
## Server
```sh
# Server 1
cd server
bazel run :server --cxxopt=-std=c++17 -- 10.10.1.3:8080 10.10.1.3:9090 /mnt/Work/CS739-P3/store1
# Server 2
cd server
bazel run :server --cxxopt=-std=c++17 -- 10.10.1.3:9090 10.10.1.3:8080 /mnt/Work/CS739-P3/store2
# Client
bazel run :client --cxxopt=-std=c++17 -- /mnt/Work/CS739-P3/resources/exec.conf
# Tests
./scripts/run-tests.sh
```

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

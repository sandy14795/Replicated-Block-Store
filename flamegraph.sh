FLAME_DIR=/mnt/Work/FlameGraph
IP=$1

echo "Cleanup"
ps -ef | grep "bazel-bin/server/server" | grep -v grep | awk '{print $2}' | xargs -r kill -9
rm -rf store1/ store2/

echo "Build"
bazel build //server:server --cxxopt=-std=c++17 --copt=-O3
bazel build //client:client_perf --cxxopt=-std=c++17 --copt=-O3

echo "Starting primary server"
bazel run //server:server --cxxopt=-std=c++17 --copt=-O3 -- $IP:8080 $IP:9090 /mnt/Work/CS739-P3/store1 &
server1pid=$!

sleep 5

echo "Starting backup server"
bazel run //server:server --cxxopt=-std=c++17 --copt=-O3 -- $IP:9090 $IP:8080 /mnt/Work/CS739-P3/store2 &
server2pid=$!

sleep 5

echo "Starting client"
bazel run //client:client_perf --cxxopt=-std=c++17 --copt=-O3 &

echo "Recording primary..."
sudo perf record -F 500 -p $server1pid -g -- sleep 40

sudo chmod 666 perf.data

echo "Stack traces"
perf script | $FLAME_DIR/stackcollapse-perf.pl > perf_logs/out.perf-folded

echo "Plot..."
$FLAME_DIR/flamegraph.pl perf_logs/out.perf-folded > perf_logs/perf.svg

echo "Cleanup"
# kill -9 $server1pid
# kill -9 $server2pid
rm perf.data
rm perf_logs/out.perf-folded
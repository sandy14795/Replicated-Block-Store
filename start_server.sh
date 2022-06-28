addr1=$1
addr2=$2
echo "Cleanup"
ps -ef | grep "bazel-bin/server/server" | grep -v grep | awk '{print $2}' | xargs -r kill -9
rm -rf store1/ store2/

echo "Build"
# bazel build //server:server --cxxopt=-std=c++17 --copt=-O3

echo "Starting primary server"
./bazel-bin/server/server $addr1 $addr2 /mnt/Work/CS739-P3/store1 &
server1pid=$!
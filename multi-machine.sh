BASE_DIR=/mnt/Work/CS739-P3/
node2=10.10.1.4:8080
node3=10.10.1.2:9090

echo "Cleanup"
ssh node-2 "ps -ef | grep \"bazel-bin/server/server\" | grep -v grep | awk '{print \$2}' | xargs -r kill -9"
ssh node-3 "ps -ef | grep \"bazel-bin/server/server\" | grep -v grep | awk '{print \$2}' | xargs -r kill -9"

echo "Starting primary"
ssh node-2 "cd $BASE_DIR && ./start_server.sh $node2 $node3" &

sleep 5

echo "Starting backup"
ssh node-3 "cd $BASE_DIR && ./start_server.sh $node3 $node2" &


echo "Starting client"
./start_client.sh

echo "Sanity check:"
mkdir -p store1
mkdir -p store2
scp node-2:$BASE_DIR/store1/log store1/log
scp node-3:$BASE_DIR/store1/log store2/log
echo "Diff:"
diff store1/log store2/log
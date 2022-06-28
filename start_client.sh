echo "Build"
bazel build //client:client_perf --cxxopt=-std=c++17 --copt=-O3

echo "Starting client"
bazel run //client:client_perf --cxxopt=-std=c++17 --copt=-O3
load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")
package(default_visibility = ["//visibility:public"])

cc_library(
  name = "blob_server_lib",
  srcs = ["blob_server.cc", "logger.cc"],
  hdrs = ["blob_server.h", "logger.h"],
  deps = [
    "//protos:blobstore_cc_grpc",
    "@com_github_grpc_grpc//:grpc++_reflection",
    "@com_github_grpc_grpc//:grpc++",
    "@com_google_absl//absl/strings",
    "@com_google_absl//absl/status",
    "//resources:utils_lib"
  ],
  copts = [
    "-std=c++17",
  ],
  linkopts = [
    "-lpthread",
  ],
)
cc_binary(
  name = "server",
  srcs = ["server_main.cc"],
  deps = [
    "//protos:blobstore_cc_grpc",
    "@com_github_grpc_grpc//:grpc++_reflection",
    "@com_github_grpc_grpc//:grpc++",
    "@com_google_absl//absl/strings",
    "@com_google_absl//absl/status",
    ":blob_server_lib"
  ],
  copts = [
    "-std=c++17",
  ],
  linkopts = [
    "-lpthread",
  ],
)
load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")
package(default_visibility = ["//visibility:public"])

cc_library(
  name = "blob_client_lib",
  srcs = ["blob_client.cc"],
  hdrs = ["blob_client.h"],
  deps = [
    "//protos:blobstore_cc_grpc",
    "@com_github_grpc_grpc//:grpc++_reflection",
    "@com_github_grpc_grpc//:grpc++",
    "@com_google_absl//absl/strings",
    "//resources:utils_lib",
  ]
)

cc_binary(
  name = "client",
  srcs = ["client_main.cc"],
  deps = [
    "//protos:blobstore_cc_grpc",
    "@com_github_grpc_grpc//:grpc++_reflection",
    "@com_github_grpc_grpc//:grpc++",
    "@com_google_absl//absl/strings",
    ":blob_client_lib",
    "//resources:utils_lib",
  ],
)

cc_binary(
  name = "client_test",
  srcs = ["client_test.cc"],
  deps = [
    "//protos:blobstore_cc_grpc",
    "@com_github_grpc_grpc//:grpc++_reflection",
    "@com_github_grpc_grpc//:grpc++",
    "@com_google_absl//absl/strings",
    ":blob_client_lib",
    "//resources:utils_lib",
  ],
  copts = [
    "-std=c++17",
  ],
)

cc_binary(
  name = "client_perf",
  srcs = ["client_perf.cc"],
  deps = [
    "//protos:blobstore_cc_grpc",
    "@com_github_grpc_grpc//:grpc++_reflection",
    "@com_github_grpc_grpc//:grpc++",
    "@com_google_absl//absl/strings",
    "@com_google_absl//absl/time",
    "@com_google_absl//absl/flags:flag",
    ":blob_client_lib",
    "//resources:utils_lib",
  ],
)

package(default_visibility = ["//visibility:public"])

load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_proto_library")
load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")


proto_library(
    name = "blobstore_proto",
    srcs = ["blobstore.proto"],
)

cc_proto_library(
    name = "blobstore_cc_proto",
    deps = [":blobstore_proto"],
)

cc_grpc_library(
    name = "blobstore_cc_grpc",
    srcs = [":blobstore_proto"],
    grpc_only = True,
    deps = [":blobstore_cc_proto"],
)
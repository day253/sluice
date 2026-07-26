#!/usr/bin/env python3

"""Validate the post-deploy control/Worker topology exposed by the FSM."""

import argparse
import json
import os
import sys


def topology_is_valid(
    nodes_payload,
    allocations_payload,
    controls,
    workers,
    max_worker_capacity,
):
    nodes = nodes_payload["nodes"]
    allocations = allocations_payload["nodes"]

    node_ids = [node["node_id"] for node in nodes]
    control_nodes = [node for node in nodes if node.get("role") == "control"]
    up_workers = [
        node
        for node in nodes
        if node.get("role") == "worker" and node.get("status") == "up"
    ]
    up_worker_ids = {node["node_id"] for node in up_workers}
    worker_capacity_by_id = {
        node["node_id"]: node.get("total_workers", 0) for node in up_workers
    }
    allocation_ids = [allocation.get("node_id") for allocation in allocations]

    def allocation_is_bounded(allocation):
        node_id = allocation.get("node_id")
        tenants = allocation.get("tenants", {})
        return (
            node_id in up_worker_ids
            and isinstance(tenants, dict)
            and all(
                isinstance(value, int) and value >= 0 for value in tenants.values()
            )
            and sum(tenants.values()) <= worker_capacity_by_id[node_id]
        )

    return (
        len(node_ids) == len(set(node_ids))
        and all(node.get("role") in {"control", "worker"} for node in nodes)
        and len(control_nodes) == controls
        and all(
            node.get("status") == "up" and node.get("total_workers") == 0
            for node in control_nodes
        )
        and len(up_workers) == workers
        and all(
            isinstance(capacity, int)
            and capacity >= 1
            and capacity <= max_worker_capacity
            for capacity in worker_capacity_by_id.values()
        )
        and len(allocation_ids) == len(set(allocation_ids))
        and all(allocation_is_bounded(allocation) for allocation in allocations)
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes-file")
    parser.add_argument("--allocations-file")
    parser.add_argument("--controls", type=int, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--max-worker-capacity", type=int, required=True)
    args = parser.parse_args()

    try:
        if bool(args.nodes_file) != bool(args.allocations_file):
            raise ValueError(
                "--nodes-file and --allocations-file must be provided together"
            )
        if args.nodes_file:
            with open(args.nodes_file, encoding="utf-8") as nodes_file:
                nodes_payload = json.load(nodes_file)
            with open(args.allocations_file, encoding="utf-8") as allocations_file:
                allocations_payload = json.load(allocations_file)
        else:
            # Retain the environment input for direct callers while the production
            # verifier uses files so large allocation snapshots never hit ARG_MAX.
            nodes_payload = json.loads(os.environ["NODES_JSON"])
            allocations_payload = json.loads(os.environ["ALLOCATIONS_JSON"])
        valid = topology_is_valid(
            nodes_payload,
            allocations_payload,
            args.controls,
            args.workers,
            args.max_worker_capacity,
        )
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        valid = False

    if valid:
        capacity = sum(
            node.get("total_workers", 0)
            for node in nodes_payload["nodes"]
            if node.get("role") == "worker" and node.get("status") == "up"
        )
        print(capacity)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())

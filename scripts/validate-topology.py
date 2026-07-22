#!/usr/bin/env python3

"""Validate the post-deploy control/Worker topology exposed by the FSM."""

import argparse
import json
import os
import sys


def topology_is_valid(nodes_payload, allocations_payload, controls, workers, worker_capacity):
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

    return (
        len(node_ids) == len(set(node_ids))
        and all(node.get("role") in {"control", "worker"} for node in nodes)
        and len(control_nodes) == controls
        and all(
            node.get("status") == "up" and node.get("total_workers") == 0
            for node in control_nodes
        )
        and len(up_workers) == workers
        and sum(node.get("total_workers", 0) for node in up_workers) == worker_capacity
        and all(
            allocation.get("node_id") in up_worker_ids for allocation in allocations
        )
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--controls", type=int, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--worker-capacity", type=int, required=True)
    args = parser.parse_args()

    try:
        nodes_payload = json.loads(os.environ["NODES_JSON"])
        allocations_payload = json.loads(os.environ["ALLOCATIONS_JSON"])
        valid = topology_is_valid(
            nodes_payload,
            allocations_payload,
            args.controls,
            args.workers,
            args.worker_capacity,
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        valid = False

    return 0 if valid else 1


if __name__ == "__main__":
    sys.exit(main())

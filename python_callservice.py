#!/usr/bin/env python3
"""Python replacement for python_callservice.sh."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from scripts.callservice_runner import (
    DEFAULT_FUNCTION_PREFIX,
    PipelineResult,
    add_common_args,
    fail,
    print_config,
    print_iteration_metrics,
    print_summary,
    resolve_backend,
    run_step,
    write_python_metrics,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    add_common_args(parser)
    parser.add_argument(
        "--transform-function",
        default=os.environ.get("PYTHON_TRANSFORM_FUNCTION", f"{DEFAULT_FUNCTION_PREFIX}-python-transform"),
    )
    parser.add_argument(
        "--load-function",
        default=os.environ.get("PYTHON_LOAD_FUNCTION", f"{DEFAULT_FUNCTION_PREFIX}-python-load"),
    )
    parser.add_argument(
        "--query-function",
        default=os.environ.get("PYTHON_QUERY_FUNCTION", f"{DEFAULT_FUNCTION_PREFIX}-python-query"),
    )
    parser.add_argument("--output-file", type=Path, default=Path("pipeline_results.csv"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        args.resolved_backend = resolve_backend(args.backend)
        input_file = f"{args.rows}SalesRecords.csv"
        print_config(args.rows, args.iterations, input_file)

        results: list[PipelineResult] = []
        for iteration in range(1, args.iterations + 1):
            print(f"Running pipeline iteration {iteration}...")

            print(f"Running Transform function - iteration {iteration}")
            transform = run_step(
                args,
                args.transform_function,
                {"body": {"bucket_name": args.bucket, "key": input_file}},
            )

            print(f"Running Load function - iteration {iteration}")
            load = run_step(
                args,
                args.load_function,
                {
                    "bucket_name": args.bucket,
                    "key": f"transformed_{input_file}",
                    "db_file_name": "data.db",
                },
            )

            print(f"Running Query function - iteration {iteration}")
            query = run_step(
                args,
                args.query_function,
                {
                    "bucket_name": args.bucket,
                    "key": "databases/data.db",
                    "Filters": {"Region": "Sub-Saharan Africa"},
                    "Group By": ["Region", "Country"],
                },
            )

            result = PipelineResult(
                iteration=iteration,
                transform_duration_ms=transform.duration_ms,
                load_duration_ms=load.duration_ms,
                query_duration_ms=query.duration_ms,
            )
            results.append(result)
            print_iteration_metrics(result, args.rows)

        write_python_metrics(args.output_file, args.rows, results)
        print_summary(args.rows, results)
        print(f"Results saved to {args.output_file}")
        return 0
    except Exception as exc:
        return fail(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())

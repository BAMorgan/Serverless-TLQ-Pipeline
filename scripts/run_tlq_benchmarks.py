#!/usr/bin/env python3
"""Run Serverless TLQ Pipeline benchmarks through AWS CLI.

This runner keeps the original experiment shape while avoiding the old
Windows-hostile Bash/jq/bc dependency chain:

- derive canonical SalesRecords inputs from data/100000 Sales Records.csv
- upload those inputs to the CDK-created S3 bucket
- invoke Transform, Load, and Query for Java and Python
- record per-step SAAF fields plus pipeline-level runtime/throughput
- write detailed CSV, raw Lambda responses, and summary CSV files
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUTS_FILE = REPO_ROOT / "infrastructure" / "cdk" / "cdk-outputs.json"
DEFAULT_SOURCE_FILE = REPO_ROOT / "data" / "100000 Sales Records.csv"
DEFAULT_RESULTS_DIR = REPO_ROOT / "benchmark_results"
DEFAULT_INPUT_DIR = REPO_ROOT / "benchmark_inputs"
DEFAULT_DATASET_SIZES = [100, 1000, 5000, 10000, 100000]
SAAF_FIELDS = [
    "version",
    "lang",
    "platform",
    "functionName",
    "functionMemory",
    "functionRegion",
    "newcontainer",
    "uuid",
    "containerID",
    "vmID",
    "vmuptime",
    "linuxVersion",
    "cpuType",
    "cpuModel",
    "cpuCores",
    "cpuUsr",
    "cpuNice",
    "cpuKrn",
    "cpuIdle",
    "cpuIowait",
    "cpuIrq",
    "cpuSoftIrq",
    "vmcpusteal",
    "contextSwitches",
    "cpuUsrDelta",
    "cpuNiceDelta",
    "cpuKrnDelta",
    "cpuIdleDelta",
    "cpuIowaitDelta",
    "cpuIrqDelta",
    "cpuSoftIrqDelta",
    "vmcpustealDelta",
    "contextSwitchesDelta",
    "totalMemory",
    "freeMemory",
    "pageFaults",
    "majorPageFaults",
    "pageFaultsDelta",
    "majorPageFaultsDelta",
    "frameworkRuntime",
    "userRuntime",
    "frameworkRuntimeDeltas",
    "runtime",
    "startTime",
    "endTime",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--outputs-file", type=Path, default=DEFAULT_OUTPUTS_FILE)
    parser.add_argument("--source-file", type=Path, default=DEFAULT_SOURCE_FILE)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-west-2"))
    parser.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    parser.add_argument("--stack-name", default="ServerlessTlqPipelineStack")
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--dataset-sizes", type=int, nargs="+", default=DEFAULT_DATASET_SIZES)
    parser.add_argument("--languages", nargs="+", choices=["java", "python"], default=["java", "python"])
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--smoke-only", action="store_true")
    parser.add_argument(
        "--lambda-price-per-gb-second",
        type=float,
        default=0.0000166667,
        help="Used only for local cost estimate columns.",
    )
    parser.add_argument(
        "--lambda-request-price-per-million",
        type=float,
        default=0.20,
        help="Used only for local cost estimate columns.",
    )
    return parser.parse_args()


def run_command(args: list[str], *, timeout: int = 1200) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    if result.returncode != 0:
        cmd = " ".join(args)
        raise RuntimeError(f"Command failed ({result.returncode}): {cmd}\n{result.stderr.strip()}")
    return result


def aws_args(args: argparse.Namespace, service_args: list[str]) -> list[str]:
    command = ["aws", *service_args, "--region", args.region]
    if args.profile:
        command.extend(["--profile", args.profile])
    return command


def load_outputs(outputs_file: Path, stack_name: str) -> dict[str, str]:
    if not outputs_file.exists():
        raise FileNotFoundError(
            f"CDK outputs file not found: {outputs_file}. Run `npm run deploy -- --outputs-file cdk-outputs.json` first."
        )
    outputs = json.loads(outputs_file.read_text(encoding="utf-8"))
    if stack_name not in outputs:
        available = ", ".join(outputs.keys())
        raise KeyError(f"Stack {stack_name!r} not in {outputs_file}. Available: {available}")
    return outputs[stack_name]


def prepare_datasets(source_file: Path, input_dir: Path, dataset_sizes: list[int]) -> list[dict[str, Any]]:
    if not source_file.exists():
        raise FileNotFoundError(f"Source data file not found: {source_file}")

    input_dir.mkdir(parents=True, exist_ok=True)
    max_rows = max(dataset_sizes)
    prepared = []

    with source_file.open("r", encoding="utf-8-sig", newline="") as source:
        header = source.readline()
        if not header:
            raise ValueError(f"Source data file is empty: {source_file}")
        rows = []
        for _ in range(max_rows):
            line = source.readline()
            if not line:
                break
            rows.append(line)

    if len(rows) < max_rows:
        raise ValueError(f"Source file only contains {len(rows)} rows, but {max_rows} were requested.")

    for size in dataset_sizes:
        key = f"{size}SalesRecords.csv"
        local_path = input_dir / key
        with local_path.open("w", encoding="utf-8", newline="") as target:
            target.write(header)
            target.writelines(rows[:size])
        prepared.append({"rows": size, "key": key, "path": local_path})

    return prepared


def upload_datasets(args: argparse.Namespace, bucket_name: str, datasets: list[dict[str, Any]]) -> None:
    for dataset in datasets:
        local_path = str(dataset["path"])
        s3_uri = f"s3://{bucket_name}/{dataset['key']}"
        print(f"Uploading {local_path} -> {s3_uri}", flush=True)
        run_command(aws_args(args, ["s3", "cp", local_path, s3_uri]), timeout=1800)


def invoke_lambda(
    args: argparse.Namespace,
    function_name: str,
    payload: dict[str, Any],
    response_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    response_path.parent.mkdir(parents=True, exist_ok=True)
    payload_json = json.dumps(payload, separators=(",", ":"))
    command = aws_args(
        args,
        [
            "lambda",
            "invoke",
            "--function-name",
            function_name,
            "--payload",
            payload_json,
            "--cli-binary-format",
            "raw-in-base64-out",
            "--cli-read-timeout",
            "900",
            "--log-type",
            "Tail",
            str(response_path),
        ],
    )
    result = run_command(command, timeout=1200)
    metadata = json.loads(result.stdout or "{}")
    response = json.loads(response_path.read_text(encoding="utf-8") or "{}")
    return metadata, response


def function_names(outputs: dict[str, str], language: str) -> dict[str, str]:
    prefix = "Java" if language == "java" else "Python"
    return {
        "transform": outputs[f"{prefix}TransformFunctionName"],
        "load": outputs[f"{prefix}LoadFunctionName"],
        "query": outputs[f"{prefix}QueryFunctionName"],
    }


def step_payload(language: str, step: str, bucket_name: str, input_key: str, db_name: str) -> dict[str, Any]:
    if step == "transform":
        return {"body": {"bucket_name": bucket_name, "key": input_key}}
    if step == "load":
        if language == "java":
            return {
                "bucket_name": bucket_name,
                "csv_file_key": f"transformed_{input_key}",
                "db_file_name": db_name,
            }
        return {
            "bucket_name": bucket_name,
            "key": f"transformed_{input_key}",
            "db_file_name": db_name,
        }
    if step == "query":
        return {
            "bucket_name": bucket_name,
            "key": f"databases/{db_name}",
            "Filters": {"Region": "Sub-Saharan Africa"},
            "Group By": ["Region", "Country"],
        }
    raise ValueError(f"Unknown step: {step}")


def number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
        if math.isnan(parsed) or math.isinf(parsed):
            return None
        return parsed
    except (TypeError, ValueError):
        return None


def status_from_response(response: dict[str, Any], metadata: dict[str, Any]) -> str:
    if metadata.get("FunctionError"):
        return "function_error"
    status_code = response.get("statusCode")
    if status_code is not None and int(status_code) >= 400:
        return "error"
    if "error" in response:
        return "error"
    return "ok"


def row_for_step(
    language: str,
    dataset_rows: int,
    iteration: int,
    step: str,
    function_name: str,
    metadata: dict[str, Any],
    response: dict[str, Any],
    price_per_gb_second: float,
    request_price_per_million: float,
) -> dict[str, Any]:
    runtime_ms = number(response.get("runtime"))
    function_memory_mb = number(response.get("functionMemory"))
    throughput = dataset_rows / (runtime_ms / 1000) if runtime_ms and runtime_ms > 0 else ""
    billed_cost = ""
    if runtime_ms and function_memory_mb:
        billed_gb_seconds = (runtime_ms / 1000) * (function_memory_mb / 1024)
        billed_cost = billed_gb_seconds * price_per_gb_second + (request_price_per_million / 1_000_000)

    row = {
        "language": language,
        "dataset_rows": dataset_rows,
        "iteration": iteration,
        "step": step,
        "function_name": function_name,
        "status": status_from_response(response, metadata),
        "aws_status_code": metadata.get("StatusCode"),
        "runtime_ms": runtime_ms if runtime_ms is not None else "",
        "runtime_seconds": runtime_ms / 1000 if runtime_ms is not None else "",
        "throughput_rows_per_second": throughput,
        "estimated_lambda_cost_usd": billed_cost,
    }

    for field in SAAF_FIELDS:
        row[f"saaf_{field}"] = response.get(field, "")

    return row


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def summarize(step_rows: list[dict[str, Any]], pipeline_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary = []
    grouped: dict[tuple[Any, ...], list[float]] = {}
    for row in step_rows:
        value = number(row.get("runtime_ms"))
        if value is None:
            continue
        key = (row["language"], row["dataset_rows"], row["step"])
        grouped.setdefault(key, []).append(value)

    for (language, dataset_rows, step), values in sorted(grouped.items()):
        values_sorted = sorted(values)
        p95_index = min(len(values_sorted) - 1, math.ceil(0.95 * len(values_sorted)) - 1)
        avg_ms = mean(values)
        summary.append(
            {
                "language": language,
                "dataset_rows": dataset_rows,
                "scope": step,
                "runs": len(values),
                "mean_runtime_ms": avg_ms,
                "stddev_runtime_ms": pstdev(values) if len(values) > 1 else 0,
                "min_runtime_ms": min(values),
                "max_runtime_ms": max(values),
                "p95_runtime_ms": values_sorted[p95_index],
                "mean_throughput_rows_per_second": dataset_rows / (avg_ms / 1000) if avg_ms else "",
            }
        )

    grouped_pipeline: dict[tuple[Any, ...], list[float]] = {}
    for row in pipeline_rows:
        value = number(row.get("total_runtime_ms"))
        if value is None:
            continue
        key = (row["language"], row["dataset_rows"])
        grouped_pipeline.setdefault(key, []).append(value)

    for (language, dataset_rows), values in sorted(grouped_pipeline.items()):
        values_sorted = sorted(values)
        p95_index = min(len(values_sorted) - 1, math.ceil(0.95 * len(values_sorted)) - 1)
        avg_ms = mean(values)
        summary.append(
            {
                "language": language,
                "dataset_rows": dataset_rows,
                "scope": "pipeline_total",
                "runs": len(values),
                "mean_runtime_ms": avg_ms,
                "stddev_runtime_ms": pstdev(values) if len(values) > 1 else 0,
                "min_runtime_ms": min(values),
                "max_runtime_ms": max(values),
                "p95_runtime_ms": values_sorted[p95_index],
                "mean_throughput_rows_per_second": dataset_rows / (avg_ms / 1000) if avg_ms else "",
            }
        )

    return summary


def copy_latest_marker(run_dir: Path, results_root: Path) -> None:
    marker = results_root / "latest.txt"
    marker.write_text(str(run_dir), encoding="utf-8")


def main() -> int:
    args = parse_args()
    outputs = load_outputs(args.outputs_file, args.stack_name)
    bucket_name = outputs["DataBucketName"]
    datasets = prepare_datasets(args.source_file, args.input_dir, args.dataset_sizes)

    if not args.skip_upload:
        upload_datasets(args, bucket_name, datasets)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = args.results_dir / timestamp
    raw_dir = run_dir / "raw_responses"
    run_dir.mkdir(parents=True, exist_ok=True)

    shutil.copyfile(args.outputs_file, run_dir / "cdk-outputs.json")
    (run_dir / "run_config.json").write_text(
        json.dumps(vars(args) | {"bucket_name": bucket_name}, indent=2, default=str),
        encoding="utf-8",
    )

    step_rows: list[dict[str, Any]] = []
    pipeline_rows: list[dict[str, Any]] = []

    active_datasets = datasets[:1] if args.smoke_only else datasets
    active_iterations = 1 if args.smoke_only else args.iterations

    for dataset in active_datasets:
        dataset_rows = int(dataset["rows"])
        input_key = str(dataset["key"])
        for language in args.languages:
            names = function_names(outputs, language)
            for iteration in range(1, active_iterations + 1):
                db_name = f"{language}_{dataset_rows}_iter_{iteration}.db"
                pipeline_runtime_ms = 0.0
                pipeline_cost = 0.0
                statuses = []

                print(
                    f"{language} rows={dataset_rows} iteration={iteration}: transform/load/query",
                    flush=True,
                )

                for step in ["transform", "load", "query"]:
                    response_path = raw_dir / language / str(dataset_rows) / f"iter_{iteration}_{step}.json"
                    metadata, response = invoke_lambda(
                        args,
                        names[step],
                        step_payload(language, step, bucket_name, input_key, db_name),
                        response_path,
                    )
                    row = row_for_step(
                        language,
                        dataset_rows,
                        iteration,
                        step,
                        names[step],
                        metadata,
                        response,
                        args.lambda_price_per_gb_second,
                        args.lambda_request_price_per_million,
                    )
                    step_rows.append(row)
                    statuses.append(row["status"])
                    runtime_ms = number(row.get("runtime_ms")) or 0
                    pipeline_runtime_ms += runtime_ms
                    pipeline_cost += number(row.get("estimated_lambda_cost_usd")) or 0

                    if row["status"] != "ok":
                        print(f"  {step} returned {row['status']}; see {response_path}", flush=True)
                        break

                pipeline_rows.append(
                    {
                        "language": language,
                        "dataset_rows": dataset_rows,
                        "iteration": iteration,
                        "status": "ok" if all(status == "ok" for status in statuses) else "error",
                        "total_runtime_ms": pipeline_runtime_ms,
                        "total_runtime_seconds": pipeline_runtime_ms / 1000,
                        "total_throughput_rows_per_second": dataset_rows / (pipeline_runtime_ms / 1000)
                        if pipeline_runtime_ms
                        else "",
                        "estimated_lambda_cost_usd": pipeline_cost,
                    }
                )

                time.sleep(1)

    write_csv(run_dir / "tlq_step_metrics.csv", step_rows)
    write_csv(run_dir / "tlq_pipeline_metrics.csv", pipeline_rows)
    write_csv(run_dir / "tlq_summary.csv", summarize(step_rows, pipeline_rows))
    copy_latest_marker(run_dir, args.results_dir)
    print(f"Benchmark outputs written to {run_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

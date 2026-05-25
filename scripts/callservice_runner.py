#!/usr/bin/env python3
"""Shared runner for the Java/Python TLQ callservice scripts."""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, stdev
from typing import Any, Iterable


DEFAULT_ROWS = 10000
DEFAULT_ITERATIONS = 10
DEFAULT_REGION = "us-west-2"
DEFAULT_FUNCTION_PREFIX = "serverless-tlq"


@dataclass(frozen=True)
class StepResult:
    duration_ms: float
    throughput: float
    response: dict[str, Any]


@dataclass(frozen=True)
class PipelineResult:
    iteration: int
    transform_duration_ms: float
    load_duration_ms: float
    query_duration_ms: float

    @property
    def total_duration_ms(self) -> float:
        return self.transform_duration_ms + self.load_duration_ms + self.query_duration_ms


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bucket", default=os.environ.get("BUCKET_NAME"))
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    parser.add_argument("--region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or DEFAULT_REGION)
    parser.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    parser.add_argument(
        "--backend",
        choices=["auto", "awscli", "boto3"],
        default="auto",
        help="Use boto3 if installed, otherwise fall back to AWS CLI.",
    )


def resolve_backend(preference: str) -> str:
    if preference == "boto3":
        require_boto3()
        return "boto3"
    if preference == "awscli":
        require_aws_cli()
        return "awscli"

    try:
        require_boto3()
        return "boto3"
    except RuntimeError:
        require_aws_cli()
    return "awscli"


def require_bucket(args: argparse.Namespace) -> None:
    if not args.bucket:
        raise RuntimeError("Set BUCKET_NAME or pass --bucket with the pipeline S3 bucket name.")


def require_boto3() -> None:
    try:
        import boto3  # noqa: F401
    except ImportError as exc:
        raise RuntimeError("boto3 is not installed. Install it with `python -m pip install boto3`.") from exc


def require_aws_cli() -> None:
    if shutil.which("aws") is None:
        raise RuntimeError("AWS CLI is not on PATH. Install AWS CLI or install boto3 and use `--backend boto3`.")


def invoke_lambda(args: argparse.Namespace, function_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    backend = args.resolved_backend
    if backend == "boto3":
        return invoke_lambda_boto3(args, function_name, payload)
    return invoke_lambda_awscli(args, function_name, payload)


def invoke_lambda_boto3(args: argparse.Namespace, function_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    session_kwargs: dict[str, str] = {}
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    if args.region:
        session_kwargs["region_name"] = args.region

    try:
        client = boto3.Session(**session_kwargs).client("lambda")
        result = client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload).encode("utf-8"),
            LogType="Tail",
        )
        response_text = result["Payload"].read().decode("utf-8")
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to invoke {function_name}: {exc}") from exc

    response = parse_lambda_payload(function_name, response_text)
    if result.get("FunctionError"):
        raise RuntimeError(f"{function_name} returned FunctionError={result['FunctionError']}: {response}")
    return response


def invoke_lambda_awscli(args: argparse.Namespace, function_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    with tempfile.NamedTemporaryFile(delete=False) as response_file:
        response_path = Path(response_file.name)

    command = [
        "aws",
        "lambda",
        "invoke",
        "--function-name",
        function_name,
        "--payload",
        json.dumps(payload, separators=(",", ":")),
        "--cli-binary-format",
        "raw-in-base64-out",
        "--log-type",
        "Tail",
        str(response_path),
    ]
    if args.region:
        command.extend(["--region", args.region])
    if args.profile:
        command.extend(["--profile", args.profile])

    try:
        result = subprocess.run(command, text=True, capture_output=True, timeout=1200)
        response_text = response_path.read_text(encoding="utf-8")
    finally:
        response_path.unlink(missing_ok=True)

    if result.returncode != 0:
        raise RuntimeError(f"Failed to invoke {function_name}:\n{result.stderr.strip()}")

    metadata = json.loads(result.stdout or "{}")
    response = parse_lambda_payload(function_name, response_text)
    if metadata.get("FunctionError"):
        raise RuntimeError(f"{function_name} returned FunctionError={metadata['FunctionError']}: {response}")
    return response


def parse_lambda_payload(function_name: str, response_text: str) -> dict[str, Any]:
    try:
        response = json.loads(response_text or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{function_name} returned non-JSON payload: {response_text!r}") from exc

    if not isinstance(response, dict):
        raise RuntimeError(f"{function_name} returned unexpected payload type: {type(response).__name__}")
    status_code = response.get("statusCode", 200)
    try:
        status_code_int = int(status_code)
    except (TypeError, ValueError):
        status_code_int = 200
    if status_code_int >= 400:
        raise RuntimeError(f"{function_name} returned statusCode={status_code}: {response}")
    if "error" in response:
        raise RuntimeError(f"{function_name} returned error: {response['error']}")
    return response


def run_step(args: argparse.Namespace, function_name: str, payload: dict[str, Any]) -> StepResult:
    response = invoke_lambda(args, function_name, payload)
    duration_ms = parse_duration_ms(function_name, response)
    throughput = calculate_throughput(args.rows, duration_ms)
    return StepResult(duration_ms=duration_ms, throughput=throughput, response=response)


def parse_duration_ms(function_name: str, response: dict[str, Any]) -> float:
    runtime = response.get("runtime")
    if runtime is None:
        raise RuntimeError(f"{function_name} response does not include SAAF `runtime`: {response}")
    try:
        return float(runtime)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{function_name} returned invalid runtime value {runtime!r}") from exc


def calculate_throughput(rows: int, duration_ms: float) -> float:
    if duration_ms <= 0:
        return 0.0
    return rows / (duration_ms / 1000)


def print_config(rows: int, iterations: int, input_file: str) -> None:
    print("Starting pipeline tests with configuration:")
    print(f"Dataset size: {rows} rows")
    print(f"Number of iterations: {iterations}")
    print(f"Input file: {input_file}")
    print("----------------------------------------")


def print_iteration_metrics(result: PipelineResult, rows: int) -> None:
    transform_throughput = calculate_throughput(rows, result.transform_duration_ms)
    load_throughput = calculate_throughput(rows, result.load_duration_ms)
    query_throughput = calculate_throughput(rows, result.query_duration_ms)
    total_throughput = calculate_throughput(rows, result.total_duration_ms)

    print(f"Metrics for iteration {result.iteration}:")
    print(f"Transform: {result.transform_duration_ms / 1000:.3f}s ({transform_throughput:.2f} rows/sec)")
    print(f"Load: {result.load_duration_ms / 1000:.3f}s ({load_throughput:.2f} rows/sec)")
    print(f"Query: {result.query_duration_ms / 1000:.3f}s ({query_throughput:.2f} rows/sec)")
    print(f"Total: {result.total_duration_ms / 1000:.3f}s ({total_throughput:.2f} rows/sec)")
    print("----------------------------------------")


def write_java_metrics(path: Path, rows: int, results: list[PipelineResult]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "iteration",
                "test_type",
                "transform_duration",
                "load_duration",
                "query_duration",
                "total_duration",
                "transform_throughput",
                "load_throughput",
                "query_throughput",
                "total_throughput",
            ]
        )
        for result in results:
            writer.writerow(
                [
                    result.iteration,
                    "warm",
                    round(result.transform_duration_ms),
                    round(result.load_duration_ms),
                    round(result.query_duration_ms),
                    round(result.total_duration_ms),
                    round(calculate_throughput(rows, result.transform_duration_ms), 2),
                    round(calculate_throughput(rows, result.load_duration_ms), 2),
                    round(calculate_throughput(rows, result.query_duration_ms), 2),
                    round(calculate_throughput(rows, result.total_duration_ms), 2),
                ]
            )


def write_python_metrics(path: Path, rows: int, results: list[PipelineResult]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "Iteration",
                "Transform Time",
                "Transform Throughput",
                "Load Time",
                "Load Throughput",
                "Query Time",
                "Query Throughput",
                "Total Time",
                "Total Throughput",
            ]
        )
        for result in results:
            writer.writerow(
                [
                    result.iteration,
                    round(result.transform_duration_ms / 1000, 3),
                    round(calculate_throughput(rows, result.transform_duration_ms), 2),
                    round(result.load_duration_ms / 1000, 3),
                    round(calculate_throughput(rows, result.load_duration_ms), 2),
                    round(result.query_duration_ms / 1000, 3),
                    round(calculate_throughput(rows, result.query_duration_ms), 2),
                    round(result.total_duration_ms / 1000, 3),
                    round(calculate_throughput(rows, result.total_duration_ms), 2),
                ]
            )


def write_summary(path: Path, rows: int, results: list[PipelineResult]) -> None:
    metric_values = {
        "Transform Duration (seconds)": [r.transform_duration_ms / 1000 for r in results],
        "Load Duration (seconds)": [r.load_duration_ms / 1000 for r in results],
        "Query Duration (seconds)": [r.query_duration_ms / 1000 for r in results],
        "Total Duration (seconds)": [r.total_duration_ms / 1000 for r in results],
        "Transform Throughput (rows/sec)": [calculate_throughput(rows, r.transform_duration_ms) for r in results],
        "Load Throughput (rows/sec)": [calculate_throughput(rows, r.load_duration_ms) for r in results],
        "Query Throughput (rows/sec)": [calculate_throughput(rows, r.query_duration_ms) for r in results],
        "Total Throughput (rows/sec)": [calculate_throughput(rows, r.total_duration_ms) for r in results],
    }
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Metric", "Mean", "Std Dev", "Min", "Max", "95th Percentile"])
        for name, values in metric_values.items():
            writer.writerow([name, mean_value(values), stddev_value(values), min(values), max(values), percentile(values, 95)])


def print_summary(rows: int, results: list[PipelineResult]) -> None:
    print(f"\nPipeline Statistics for {rows} rows:")
    print("\nDuration Statistics (in seconds):")
    print_stats("Transform", [r.transform_duration_ms / 1000 for r in results])
    print_stats("Load", [r.load_duration_ms / 1000 for r in results])
    print_stats("Query", [r.query_duration_ms / 1000 for r in results])
    print_stats("Total", [r.total_duration_ms / 1000 for r in results])

    print("\nThroughput Statistics (rows/second):")
    print_stats("Transform", [calculate_throughput(rows, r.transform_duration_ms) for r in results])
    print_stats("Load", [calculate_throughput(rows, r.load_duration_ms) for r in results])
    print_stats("Query", [calculate_throughput(rows, r.query_duration_ms) for r in results])
    print_stats("Total", [calculate_throughput(rows, r.total_duration_ms) for r in results])


def print_stats(name: str, values: list[float]) -> None:
    print(f"\n{name}:")
    print(f"  Average: {mean_value(values):.3f}")
    print(f"  Std Dev: {stddev_value(values):.3f}")
    print(f"  Min: {min(values):.3f}")
    print(f"  Max: {max(values):.3f}")
    print(f"  95th percentile: {percentile(values, 95):.3f}")


def mean_value(values: list[float]) -> float:
    return mean(values) if values else 0.0


def stddev_value(values: list[float]) -> float:
    return stdev(values) if len(values) > 1 else 0.0


def percentile(values: Iterable[float], percent: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = round((percent / 100) * (len(ordered) - 1))
    return ordered[index]


def fail(message: str) -> int:
    print(f"error: {message}", file=sys.stderr)
    return 1

#!/usr/bin/env python3
"""Run the Java and Python TLQ Lambda benchmark pipelines.

This is a Python replacement for the original java_callservice.sh and
python_callservice.sh scripts. It preserves their legacy CSV outputs while
also writing a clean combined metrics file with SAAF runtime, client
round-trip time, throughput, memory, cold/warm flag, and estimated Lambda cost.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, stdev
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUTS_FILE = REPO_ROOT / "infrastructure" / "cdk" / "cdk-outputs.json"
DEFAULT_STACK_NAME = "ServerlessTlqPipelineStack"
DEFAULT_REGION = "us-west-2"
DEFAULT_PREFIX = "serverless-tlq"
DEFAULT_ROWS = 10000
DEFAULT_ITERATIONS = 10
DEFAULT_MEMORY_MB = 256
LAMBDA_GB_SECOND_PRICE = 0.0000166667
LAMBDA_REQUEST_PRICE = 0.20 / 1_000_000


@dataclass(frozen=True)
class RunMetadata:
    benchmark_run_id: str
    implementation_id: str
    design_id: str


@dataclass(frozen=True)
class FunctionSet:
    transform: str
    load: str
    query: str


@dataclass(frozen=True)
class StepMetric:
    record_id: str
    benchmark_run_id: str
    implementation_id: str
    design_id: str
    language: str
    iteration: int
    stage: str
    function_name: str
    runtime_ms: float
    round_trip_ms: float
    throughput_rows_per_second: float
    function_memory_mb: int
    estimated_cost_usd: float
    newcontainer: str
    platform: str


@dataclass(frozen=True)
class PipelineMetric:
    record_id: str
    benchmark_run_id: str
    implementation_id: str
    design_id: str
    language: str
    iteration: int
    transform_ms: float
    load_ms: float
    query_ms: float

    @property
    def total_ms(self) -> float:
        return self.transform_ms + self.load_ms + self.query_ms


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--language", choices=["all", "java", "python"], default="all")
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    parser.add_argument("--region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or DEFAULT_REGION)
    parser.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    parser.add_argument("--bucket", default=os.environ.get("BUCKET_NAME"))
    parser.add_argument("--client-read-timeout", type=int, default=900)
    parser.add_argument("--client-connect-timeout", type=int, default=10)
    parser.add_argument(
        "--run-id",
        default=os.environ.get("BENCHMARK_RUN_ID"),
        help="Optional benchmark run ID. Defaults to a timestamp plus short UUID.",
    )
    parser.add_argument(
        "--implementation-id",
        default=os.environ.get("IMPLEMENTATION_ID", "current"),
        help="Label for the implementation under test, for example java-baseline or python-query-v2.",
    )
    parser.add_argument(
        "--design-id",
        default=os.environ.get("DESIGN_ID", "default"),
        help="Label for the design/change set being tested, for example temp-cleanup or sqlite-indexes.",
    )
    parser.add_argument("--outputs-file", type=Path, default=DEFAULT_OUTPUTS_FILE)
    parser.add_argument("--stack-name", default=DEFAULT_STACK_NAME)
    parser.add_argument("--input-file", type=Path)
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--java-output", type=Path, default=Path("java_pipeline_metrics.csv"))
    parser.add_argument("--python-output", type=Path, default=Path("pipeline_results.csv"))
    parser.add_argument("--java-summary-output", type=Path, default=Path("summary_statistics.csv"))
    parser.add_argument("--clean-output", type=Path, default=Path("callservice_clean_metrics.csv"))
    parser.add_argument("--clean-summary-output", type=Path, default=Path("callservice_clean_summary.csv"))
    parser.add_argument(
        "--history-output",
        type=Path,
        default=Path("benchmark_results") / "callservice_metrics_history.csv",
        help="Append-only metric history used to compare benchmark runs over time.",
    )
    parser.add_argument(
        "--summary-history-output",
        type=Path,
        default=Path("benchmark_results") / "callservice_summary_history.csv",
        help="Append-only summary history used to compare benchmark runs over time.",
    )
    parser.add_argument(
        "--legacy-java-query-key",
        action="store_true",
        help="Use the original Java script query key databases/data.db instead of the DB created in the same iteration.",
    )
    parser.add_argument(
        "--legacy-db-names",
        action="store_true",
        help="Use the original data.db/data_N.db names instead of isolated per-run database names.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        session = make_session(args)
        client_config = Config(
            connect_timeout=args.client_connect_timeout,
            read_timeout=args.client_read_timeout,
            retries={"total_max_attempts": 1},
        )
        lambda_client = session.client("lambda", region_name=args.region, config=client_config)
        s3_client = session.client("s3", region_name=args.region, config=client_config)

        outputs = load_cdk_outputs(args.outputs_file, args.stack_name)
        bucket = resolve_bucket(args.bucket, outputs)
        functions = resolve_functions(outputs)
        languages = ["java", "python"] if args.language == "all" else [args.language]
        input_path = resolve_input_file(args.rows, args.input_file)
        input_key = f"{args.rows}SalesRecords.csv"
        run_metadata = RunMetadata(
            benchmark_run_id=resolve_run_id(args.run_id),
            implementation_id=sanitize_id(args.implementation_id),
            design_id=sanitize_id(args.design_id),
        )

        print_configuration(args, bucket, input_key, input_path, run_metadata)
        if not args.skip_upload:
            print(f"Uploading {input_path} to s3://{bucket}/{input_key}")
            s3_client.upload_file(str(input_path), bucket, input_key)

        step_metrics: list[StepMetric] = []
        pipeline_metrics: list[PipelineMetric] = []

        for language in languages:
            print(f"\nStarting {language} pipeline tests...")
            for iteration in range(1, args.iterations + 1):
                pipeline = run_pipeline_iteration(
                    args=args,
                    lambda_client=lambda_client,
                    bucket=bucket,
                    input_key=input_key,
                    language=language,
                    function_set=functions[language],
                    iteration=iteration,
                    run_metadata=run_metadata,
                )
                step_metrics.extend(pipeline[0])
                pipeline_metrics.append(pipeline[1])
                print_iteration_metrics(pipeline[1], args.rows)

            language_results = [metric for metric in pipeline_metrics if metric.language == language]
            print_summary(language, args.rows, language_results)

        java_results = [metric for metric in pipeline_metrics if metric.language == "java"]
        python_results = [metric for metric in pipeline_metrics if metric.language == "python"]
        if java_results:
            write_java_legacy_csv(args.java_output, args.rows, java_results)
            write_legacy_summary_csv(args.java_summary_output, args.rows, java_results)
        if python_results:
            write_python_legacy_csv(args.python_output, args.rows, python_results)

        write_clean_summary_csv(args.clean_summary_output, args.rows, pipeline_metrics, step_metrics, run_metadata)
        write_clean_metrics_csv(args.clean_output, step_metrics)
        write_clean_metrics_csv(args.history_output, step_metrics, append=True)
        write_clean_summary_csv(
            args.summary_history_output,
            args.rows,
            pipeline_metrics,
            step_metrics,
            run_metadata,
            append=True,
        )

        print("\nWrote output files:")
        for path in output_paths(args, java_results, python_results):
            print(f"  {path}")
        return 0
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def make_session(args: argparse.Namespace) -> boto3.Session:
    kwargs: dict[str, str] = {}
    if args.profile:
        kwargs["profile_name"] = args.profile
    if args.region:
        kwargs["region_name"] = args.region
    return boto3.Session(**kwargs)


def load_cdk_outputs(path: Path, stack_name: str) -> dict[str, str]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if stack_name in raw:
        return raw[stack_name]
    if all(isinstance(value, str) for value in raw.values()):
        return raw
    return {}


def resolve_functions(outputs: dict[str, str]) -> dict[str, FunctionSet]:
    return {
        "java": FunctionSet(
            transform=outputs.get("JavaTransformFunctionName", f"{DEFAULT_PREFIX}-java-transform"),
            load=outputs.get("JavaLoadFunctionName", f"{DEFAULT_PREFIX}-java-load"),
            query=outputs.get("JavaQueryFunctionName", f"{DEFAULT_PREFIX}-java-query"),
        ),
        "python": FunctionSet(
            transform=outputs.get("PythonTransformFunctionName", f"{DEFAULT_PREFIX}-python-transform"),
            load=outputs.get("PythonLoadFunctionName", f"{DEFAULT_PREFIX}-python-load"),
            query=outputs.get("PythonQueryFunctionName", f"{DEFAULT_PREFIX}-python-query"),
        ),
    }


def resolve_bucket(explicit_bucket: str | None, outputs: dict[str, str]) -> str:
    bucket = explicit_bucket or outputs.get("DataBucketName")
    if bucket:
        return bucket
    raise RuntimeError(
        "No S3 bucket configured. Pass --bucket, set BUCKET_NAME, or deploy CDK with "
        "--outputs-file infrastructure/cdk/cdk-outputs.json."
    )


def resolve_input_file(rows: int, explicit_path: Path | None) -> Path:
    candidates = []
    if explicit_path is not None:
        candidates.append(explicit_path)
    candidates.extend(
        [
            REPO_ROOT / "benchmark_inputs" / f"{rows}SalesRecords.csv",
            REPO_ROOT / "data" / f"{rows} Sales Records.csv",
            REPO_ROOT / f"{rows}SalesRecords.csv",
        ]
    )
    for path in candidates:
        if path.exists():
            return path
    checked = "\n  ".join(str(path) for path in candidates)
    raise FileNotFoundError(f"Could not find input CSV for {rows} rows. Checked:\n  {checked}")


def resolve_run_id(explicit_run_id: str | None) -> str:
    if explicit_run_id:
        return sanitize_id(explicit_run_id)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{uuid.uuid4().hex[:8]}"


def sanitize_id(value: str) -> str:
    sanitized = "".join(char if char.isalnum() or char in "._-" else "_" for char in value.strip())
    return sanitized.strip("._-") or "default"


def metric_record_id(run_id: str, language: str, iteration: int, scope: str) -> str:
    return f"{run_id}_{language}_{iteration:03d}_{sanitize_id(scope)}"


def summary_record_id(run_id: str, language: str, scope: str) -> str:
    return f"{run_id}_{language}_summary_{sanitize_id(scope)}"


def print_configuration(
    args: argparse.Namespace,
    bucket: str,
    input_key: str,
    input_path: Path,
    run_metadata: RunMetadata,
) -> None:
    print("Starting pipeline tests with configuration:")
    print(f"Benchmark run ID: {run_metadata.benchmark_run_id}")
    print(f"Implementation ID: {run_metadata.implementation_id}")
    print(f"Design ID: {run_metadata.design_id}")
    print(f"Dataset size: {args.rows} rows")
    print(f"Number of iterations: {args.iterations}")
    print(f"Input file: {input_key}")
    print(f"Local input path: {input_path}")
    print(f"S3 bucket: {bucket}")
    print(f"AWS region: {args.region}")
    print("----------------------------------------")


def run_pipeline_iteration(
    *,
    args: argparse.Namespace,
    lambda_client: Any,
    bucket: str,
    input_key: str,
    language: str,
    function_set: FunctionSet,
    iteration: int,
    run_metadata: RunMetadata,
) -> tuple[list[StepMetric], PipelineMetric]:
    db_name = db_name_for(language, iteration, run_metadata.benchmark_run_id, legacy=args.legacy_db_names)
    if language == "java":
        load_payload = {"bucket_name": bucket, "csv_file_key": f"transformed_{input_key}", "db_file_name": db_name}
    else:
        load_payload = {"bucket_name": bucket, "key": f"transformed_{input_key}", "db_file_name": db_name}

    query_key = "databases/data.db" if args.legacy_java_query_key and language == "java" else f"databases/{db_name}"
    calls = [
        ("transform", function_set.transform, {"body": {"bucket_name": bucket, "key": input_key}}),
        ("load", function_set.load, load_payload),
        (
            "query",
            function_set.query,
            {
                "bucket_name": bucket,
                "key": query_key,
                "Filters": {"Region": "Sub-Saharan Africa"},
                "Group By": ["Region", "Country"],
            },
        ),
    ]

    print(f"Running pipeline iteration {iteration} ({language})...")
    metrics = []
    for stage, function_name, payload in calls:
        print(f"Running {stage.title()} function - iteration {iteration}")
        metrics.append(
            invoke_stage(
                lambda_client,
                args.rows,
                run_metadata,
                language,
                iteration,
                stage,
                function_name,
                payload,
            )
        )

    by_stage = {metric.stage: metric for metric in metrics}
    pipeline_metric = PipelineMetric(
        record_id=metric_record_id(run_metadata.benchmark_run_id, language, iteration, "pipeline"),
        benchmark_run_id=run_metadata.benchmark_run_id,
        implementation_id=run_metadata.implementation_id,
        design_id=run_metadata.design_id,
        language=language,
        iteration=iteration,
        transform_ms=by_stage["transform"].runtime_ms,
        load_ms=by_stage["load"].runtime_ms,
        query_ms=by_stage["query"].runtime_ms,
    )
    return metrics, pipeline_metric


def db_name_for(language: str, iteration: int, run_id: str, *, legacy: bool) -> str:
    if legacy:
        if language == "java":
            return f"data_{iteration}.db"
        return "data.db"
    return f"{run_id}_{language}_data_{iteration}.db"


def invoke_stage(
    lambda_client: Any,
    rows: int,
    run_metadata: RunMetadata,
    language: str,
    iteration: int,
    stage: str,
    function_name: str,
    payload: dict[str, Any],
) -> StepMetric:
    start = time.perf_counter()
    try:
        result = lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload).encode("utf-8"),
            LogType="Tail",
        )
        response_text = result["Payload"].read().decode("utf-8")
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to invoke {function_name}: {exc}") from exc
    round_trip_ms = (time.perf_counter() - start) * 1000

    response = parse_lambda_response(function_name, result, response_text)
    runtime_ms = required_float(response, "runtime", function_name)
    memory_mb = int(float(response.get("functionMemory") or DEFAULT_MEMORY_MB))
    return StepMetric(
        record_id=metric_record_id(run_metadata.benchmark_run_id, language, iteration, stage),
        benchmark_run_id=run_metadata.benchmark_run_id,
        implementation_id=run_metadata.implementation_id,
        design_id=run_metadata.design_id,
        language=language,
        iteration=iteration,
        stage=stage,
        function_name=function_name,
        runtime_ms=runtime_ms,
        round_trip_ms=round_trip_ms,
        throughput_rows_per_second=throughput(rows, runtime_ms),
        function_memory_mb=memory_mb,
        estimated_cost_usd=estimate_lambda_cost(runtime_ms, memory_mb),
        newcontainer=str(response.get("newcontainer", "")),
        platform=str(response.get("platform", "")),
    )


def parse_lambda_response(function_name: str, result: dict[str, Any], response_text: str) -> dict[str, Any]:
    try:
        response = json.loads(response_text or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{function_name} returned non-JSON payload: {response_text!r}") from exc
    if not isinstance(response, dict):
        raise RuntimeError(f"{function_name} returned unexpected payload type: {type(response).__name__}")
    if result.get("FunctionError"):
        raise RuntimeError(f"{function_name} returned FunctionError={result['FunctionError']}: {response}")
    status_code = response.get("statusCode")
    if status_code is not None:
        try:
            if int(status_code) >= 400:
                raise RuntimeError(f"{function_name} returned statusCode={status_code}: {response}")
        except ValueError:
            pass
    if "error" in response:
        raise RuntimeError(f"{function_name} returned error: {response['error']}")
    if "runtime" not in response:
        raise RuntimeError(f"{function_name} response did not include SAAF runtime: {response}")
    return response


def required_float(response: dict[str, Any], key: str, function_name: str) -> float:
    try:
        return float(response[key])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"{function_name} returned invalid {key}: {response.get(key)!r}") from exc


def throughput(rows: int, runtime_ms: float) -> float:
    if runtime_ms <= 0:
        return 0.0
    return rows / (runtime_ms / 1000)


def estimate_lambda_cost(runtime_ms: float, memory_mb: int) -> float:
    gb_seconds = (runtime_ms / 1000) * (memory_mb / 1024)
    return gb_seconds * LAMBDA_GB_SECOND_PRICE + LAMBDA_REQUEST_PRICE


def print_iteration_metrics(metric: PipelineMetric, rows: int) -> None:
    print(f"Metrics for iteration {metric.iteration}:")
    print(f"Transform: {metric.transform_ms / 1000:.3f}s ({throughput(rows, metric.transform_ms):.2f} rows/sec)")
    print(f"Load: {metric.load_ms / 1000:.3f}s ({throughput(rows, metric.load_ms):.2f} rows/sec)")
    print(f"Query: {metric.query_ms / 1000:.3f}s ({throughput(rows, metric.query_ms):.2f} rows/sec)")
    print(f"Total: {metric.total_ms / 1000:.3f}s ({throughput(rows, metric.total_ms):.2f} rows/sec)")
    print("----------------------------------------")


def print_summary(language: str, rows: int, metrics: list[PipelineMetric]) -> None:
    print(f"\nPipeline Statistics for {language} - {rows} rows:")
    print("\nDuration Statistics (in seconds):")
    print_stats("Transform", [metric.transform_ms / 1000 for metric in metrics], precision=3)
    print_stats("Load", [metric.load_ms / 1000 for metric in metrics], precision=3)
    print_stats("Query", [metric.query_ms / 1000 for metric in metrics], precision=3)
    print_stats("Total", [metric.total_ms / 1000 for metric in metrics], precision=3)
    print("\nThroughput Statistics (rows/second):")
    print_stats("Transform", [throughput(rows, metric.transform_ms) for metric in metrics], precision=2)
    print_stats("Load", [throughput(rows, metric.load_ms) for metric in metrics], precision=2)
    print_stats("Query", [throughput(rows, metric.query_ms) for metric in metrics], precision=2)
    print_stats("Total", [throughput(rows, metric.total_ms) for metric in metrics], precision=2)


def print_stats(label: str, values: list[float], *, precision: int) -> None:
    print(f"\n{label}:")
    print(f"  Average: {average(values):.{precision}f}")
    print(f"  Std Dev: {std_dev(values):.{precision}f}")
    print(f"  Min: {min(values):.{precision}f}")
    print(f"  Max: {max(values):.{precision}f}")
    print(f"  95th percentile: {percentile(values, 95):.{precision}f}")


def write_java_legacy_csv(path: Path, rows: int, metrics: list[PipelineMetric]) -> None:
    ensure_parent_dir(path)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "record_id",
                "benchmark_run_id",
                "implementation_id",
                "design_id",
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
        for metric in metrics:
            writer.writerow(
                [
                    metric.record_id,
                    metric.benchmark_run_id,
                    metric.implementation_id,
                    metric.design_id,
                    metric.iteration,
                    "warm",
                    round(metric.transform_ms),
                    round(metric.load_ms),
                    round(metric.query_ms),
                    round(metric.total_ms),
                    round(throughput(rows, metric.transform_ms), 2),
                    round(throughput(rows, metric.load_ms), 2),
                    round(throughput(rows, metric.query_ms), 2),
                    round(throughput(rows, metric.total_ms), 2),
                ]
            )


def write_python_legacy_csv(path: Path, rows: int, metrics: list[PipelineMetric]) -> None:
    ensure_parent_dir(path)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "Record ID",
                "Benchmark Run ID",
                "Implementation ID",
                "Design ID",
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
        for metric in metrics:
            writer.writerow(
                [
                    metric.record_id,
                    metric.benchmark_run_id,
                    metric.implementation_id,
                    metric.design_id,
                    metric.iteration,
                    round(metric.transform_ms / 1000, 3),
                    round(throughput(rows, metric.transform_ms), 2),
                    round(metric.load_ms / 1000, 3),
                    round(throughput(rows, metric.load_ms), 2),
                    round(metric.query_ms / 1000, 3),
                    round(throughput(rows, metric.query_ms), 2),
                    round(metric.total_ms / 1000, 3),
                    round(throughput(rows, metric.total_ms), 2),
                ]
            )


def write_legacy_summary_csv(path: Path, rows: int, metrics: list[PipelineMetric]) -> None:
    ensure_parent_dir(path)
    if not metrics:
        return
    columns = {
        "transform (seconds)": [metric.transform_ms / 1000 for metric in metrics],
        "load (seconds)": [metric.load_ms / 1000 for metric in metrics],
        "query (seconds)": [metric.query_ms / 1000 for metric in metrics],
        "total (seconds)": [metric.total_ms / 1000 for metric in metrics],
        "transform (rows/sec)": [throughput(rows, metric.transform_ms) for metric in metrics],
        "load (rows/sec)": [throughput(rows, metric.load_ms) for metric in metrics],
        "query (rows/sec)": [throughput(rows, metric.query_ms) for metric in metrics],
        "total (rows/sec)": [throughput(rows, metric.total_ms) for metric in metrics],
    }
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["record_id", "benchmark_run_id", "implementation_id", "design_id", "metric"] + list(columns.keys()))
        first_metric = metrics[0]
        for label, fn in [
            ("Mean", average),
            ("Std Dev", std_dev),
            ("Min", min),
            ("Max", max),
            ("95th Percentile", lambda values: percentile(values, 95)),
        ]:
            writer.writerow(
                [
                    summary_record_id(first_metric.benchmark_run_id, "java", label),
                    first_metric.benchmark_run_id,
                    first_metric.implementation_id,
                    first_metric.design_id,
                    label,
                ]
                + [round(fn(values), 3 if "seconds" in name else 2) for name, values in columns.items()]
            )


def write_clean_metrics_csv(path: Path, metrics: list[StepMetric], *, append: bool = False) -> None:
    ensure_parent_dir(path)
    write_header = should_write_header(path, append)
    with path.open("a" if append else "w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "record_id",
                "benchmark_run_id",
                "implementation_id",
                "design_id",
                "language",
                "iteration",
                "stage",
                "function_name",
                "runtime_ms",
                "runtime_seconds",
                "round_trip_ms",
                "throughput_rows_per_second",
                "function_memory_mb",
                "estimated_cost_usd",
                "newcontainer",
                "platform",
            ],
        )
        if write_header:
            writer.writeheader()
        for metric in metrics:
            writer.writerow(
                {
                    "record_id": metric.record_id,
                    "benchmark_run_id": metric.benchmark_run_id,
                    "implementation_id": metric.implementation_id,
                    "design_id": metric.design_id,
                    "language": metric.language,
                    "iteration": metric.iteration,
                    "stage": metric.stage,
                    "function_name": metric.function_name,
                    "runtime_ms": round(metric.runtime_ms, 3),
                    "runtime_seconds": round(metric.runtime_ms / 1000, 6),
                    "round_trip_ms": round(metric.round_trip_ms, 3),
                    "throughput_rows_per_second": round(metric.throughput_rows_per_second, 2),
                    "function_memory_mb": metric.function_memory_mb,
                    "estimated_cost_usd": f"{metric.estimated_cost_usd:.12f}",
                    "newcontainer": metric.newcontainer,
                    "platform": metric.platform,
                }
            )


def write_clean_summary_csv(
    path: Path,
    rows: int,
    pipeline_metrics: list[PipelineMetric],
    step_metrics: list[StepMetric],
    run_metadata: RunMetadata,
    *,
    append: bool = False,
) -> None:
    ensure_parent_dir(path)
    write_header = should_write_header(path, append)
    with path.open("a" if append else "w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "record_id",
                "benchmark_run_id",
                "implementation_id",
                "design_id",
                "language",
                "scope",
                "dataset_rows",
                "runs",
                "mean_runtime_ms",
                "stddev_runtime_ms",
                "min_runtime_ms",
                "max_runtime_ms",
                "p95_runtime_ms",
                "mean_throughput_rows_per_second",
                "estimated_cost_per_100k_usd",
            ],
        )
        if write_header:
            writer.writeheader()
        for language in sorted({metric.language for metric in pipeline_metrics}):
            for stage in ["transform", "load", "query"]:
                values = [metric.runtime_ms for metric in step_metrics if metric.language == language and metric.stage == stage]
                costs = [metric.estimated_cost_usd for metric in step_metrics if metric.language == language and metric.stage == stage]
                write_summary_row(writer, run_metadata, language, stage, rows, values, costs)
            totals = [metric.total_ms for metric in pipeline_metrics if metric.language == language]
            total_costs = []
            for pipeline_metric in [metric for metric in pipeline_metrics if metric.language == language]:
                total_costs.append(
                    sum(
                        step.estimated_cost_usd
                        for step in step_metrics
                        if step.language == language and step.iteration == pipeline_metric.iteration
                    )
                )
            write_summary_row(writer, run_metadata, language, "pipeline_total", rows, totals, total_costs)


def write_summary_row(
    writer: csv.DictWriter[str],
    run_metadata: RunMetadata,
    language: str,
    scope: str,
    rows: int,
    runtimes_ms: list[float],
    costs: list[float],
) -> None:
    avg_runtime = average(runtimes_ms)
    writer.writerow(
        {
            "record_id": summary_record_id(run_metadata.benchmark_run_id, language, scope),
            "benchmark_run_id": run_metadata.benchmark_run_id,
            "implementation_id": run_metadata.implementation_id,
            "design_id": run_metadata.design_id,
            "language": language,
            "scope": scope,
            "dataset_rows": rows,
            "runs": len(runtimes_ms),
            "mean_runtime_ms": round(avg_runtime, 3),
            "stddev_runtime_ms": round(std_dev(runtimes_ms), 3),
            "min_runtime_ms": round(min(runtimes_ms), 3),
            "max_runtime_ms": round(max(runtimes_ms), 3),
            "p95_runtime_ms": round(percentile(runtimes_ms, 95), 3),
            "mean_throughput_rows_per_second": round(throughput(rows, avg_runtime), 2),
            "estimated_cost_per_100k_usd": round(average(costs) * 100_000, 6),
        }
    )


def output_paths(args: argparse.Namespace, java_metrics: list[PipelineMetric], python_metrics: list[PipelineMetric]) -> list[Path]:
    paths = [args.clean_output, args.clean_summary_output, args.history_output, args.summary_history_output]
    if java_metrics:
        paths.extend([args.java_output, args.java_summary_output])
    if python_metrics:
        paths.append(args.python_output)
    return paths


def ensure_parent_dir(path: Path) -> None:
    if path.parent != Path("."):
        path.parent.mkdir(parents=True, exist_ok=True)


def should_write_header(path: Path, append: bool) -> bool:
    return not append or not path.exists() or path.stat().st_size == 0


def average(values: list[float]) -> float:
    return mean(values) if values else 0.0


def std_dev(values: list[float]) -> float:
    return stdev(values) if len(values) > 1 else 0.0


def percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil((percent / 100) * len(ordered)) - 1)
    return ordered[index]


if __name__ == "__main__":
    raise SystemExit(main())

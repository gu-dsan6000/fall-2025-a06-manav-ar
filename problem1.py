
import os
import argparse
import random
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, input_file_name, to_timestamp

LOG_LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]

def ensure_output_dir(output_dir):
    os.makedirs(output_dir, exist_ok=True)

def build_spark(master_url=None, app_name="problem1_log_levels", requester_pays=False):
    builder = SparkSession.builder.appName(app_name)
    if master_url:
        builder = builder.master(master_url)
    builder = builder.config("spark.hadoop.fs.s3a.aws.credentials.provider",
                             "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
    builder = builder.config("spark.hadoop.fs.s3a.requester.pays", str(requester_pays).lower())
    return builder.getOrCreate()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--master", help="Spark master URL (spark://...)")
    parser.add_argument("--s3_path", help="S3 path to log files (s3a://...)")
    parser.add_argument("--output_dir", default="data/output", help="Directory to write outputs")
    parser.add_argument("--requester_pays", action="store_true", help="Use if bucket is requester pays")
    parser.add_argument("--skip_spark", action="store_true", help="Run locally without Spark")
    parser.add_argument("--data_path", default="data/sample/*", help="Local path if skipping Spark")
    args = parser.parse_args()

    ensure_output_dir(args.output_dir)

    if args.skip_spark:
        spark = SparkSession.builder.master("local[*]").appName("problem1_local").getOrCreate()
        input_path = args.data_path
    else:
        if not args.s3_path:
            raise ValueError("Must provide --s3_path when not skipping Spark")
        spark = build_spark(master_url=args.master, requester_pays=args.requester_pays)
        input_path = args.s3_path

    print(f"[INFO] Reading logs from {input_path}")
    logs_df = spark.read.text(input_path)
    logs_df = logs_df.withColumn("file_path", input_file_name())

    logs_df = logs_df.withColumn("timestamp_str", regexp_extract(col("value"), r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1))
    logs_df = logs_df.withColumn("timestamp", to_timestamp("timestamp_str", "yy/MM/dd HH:mm:ss"))
    logs_df = logs_df.withColumn("log_level", regexp_extract(col("value"), r'\b(INFO|WARN|ERROR|DEBUG)\b', 1))

    total_lines = logs_df.count()
    lines_with_level = logs_df.filter(col("log_level") != "").count()

    counts_df = logs_df.groupBy("log_level").count()
    counts = {row["log_level"] if row["log_level"] else "UNKNOWN": row["count"] for row in counts_df.collect()}
    for lvl in LOG_LEVELS:
        counts.setdefault(lvl, 0)
    counts.setdefault("UNKNOWN", 0)


    counts_csv_path = os.path.join(args.output_dir, "problem1_counts.csv")
    with open(counts_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["log_level", "count"])
        for lvl in LOG_LEVELS + ["UNKNOWN"]:
            writer.writerow([lvl, counts.get(lvl, 0)])
    print(f"[INFO] Wrote {counts_csv_path}")

    sampled_df = logs_df.filter(col("log_level") != "").select("value", "log_level").sample(False, 0.001, seed=42)
    sampled_local = sampled_df.limit(200).collect()
    random.seed(42)
    sampled_local = random.sample(sampled_local, min(10, len(sampled_local)))

    sample_csv_path = os.path.join(args.output_dir, "problem1_sample.csv")
    with open(sample_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["log_entry", "log_level"])
        for r in sampled_local:
            entry = r["value"].replace("\n", " ").strip()
            writer.writerow([entry, r["log_level"]])
    print(f"[INFO] Wrote {sample_csv_path}")


    unique_levels_found = len([k for k in counts.keys() if counts[k] > 0 and k != "UNKNOWN"])
    summary_lines = [
        f"Total log lines processed: {total_lines:,}",
        f"Total lines with log levels: {lines_with_level:,}",
        f"Unique log levels found: {unique_levels_found}",
        "",
        "Log level distribution:"
    ]
    for lvl in LOG_LEVELS + ["UNKNOWN"]:
        cnt = counts.get(lvl, 0)
        pct = (cnt / lines_with_level * 100) if lines_with_level > 0 else 0.0
        summary_lines.append(f"  {lvl:5s}: {cnt:10,d} ({pct:6.2f}%)")

    summary_path = os.path.join(args.output_dir, "problem1_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines))
    print(f"[INFO] Wrote {summary_path}")

    spark.stop()
    print("[INFO] Done!")

if __name__ == "__main__":
    main()

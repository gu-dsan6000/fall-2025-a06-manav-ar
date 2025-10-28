import os
import argparse
import re
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, input_file_name, to_timestamp

def ensure_output_dir(output_dir):
    os.makedirs(output_dir, exist_ok=True)

def build_spark(master_url=None, app_name="problem2_cluster_usage", requester_pays=False):
    builder = SparkSession.builder.appName(app_name)
    if master_url:
        builder = builder.master(master_url)
    builder = builder.config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"
    )
    builder = builder.config(
        "spark.hadoop.fs.s3a.requester.pays", str(requester_pays).lower()
    )
    return builder.getOrCreate()

def parse_logs_to_df(logs_df):

    # Basic regex patterns
    cluster_regex = r"cluster_id=(\d+)"
    app_regex = r"application_id=(application_\d+_\d+)"
    start_regex = r"start=([\d-]+\s[\d:]+)"
    end_regex = r"end=([\d-]+\s[\d:]+)"

    df = logs_df.withColumn("cluster_id", regexp_extract(col("value"), cluster_regex, 1))
    df = df.withColumn("application_id", regexp_extract(col("value"), app_regex, 1))
    df = df.withColumn("start_time", regexp_extract(col("value"), start_regex, 1))
    df = df.withColumn("end_time", regexp_extract(col("value"), end_regex, 1))


    df = df.withColumn("start_time", to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("end_time", to_timestamp("end_time", "yyyy-MM-dd HH:mm:ss"))


    df = df.filter(
        (col("cluster_id") != "") &
        (col("application_id") != "") &
        (col("start_time").isNotNull()) &
        (col("end_time").isNotNull())
    )
    return df.select("cluster_id", "application_id", "start_time", "end_time")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--master", help="Spark master URL")
    parser.add_argument("--s3_path", help="S3 path to logs")
    parser.add_argument("--output_dir", default="data/output")
    parser.add_argument("--requester_pays", action="store_true")
    parser.add_argument("--skip_spark", action="store_true")
    parser.add_argument("--data_path", default="data/sample/*")
    args = parser.parse_args()

    ensure_output_dir(args.output_dir)

    if args.skip_spark:
        df = pd.read_csv(os.path.join(args.data_path))
        df["start_time"] = pd.to_datetime(df["start_time"])
        df["end_time"] = pd.to_datetime(df["end_time"])
    else:
        if not args.s3_path:
            raise ValueError("Must provide --s3_path when not skipping Spark")
        spark = build_spark(master_url=args.master, requester_pays=args.requester_pays)
        logs_df = spark.read.text(args.s3_path).withColumn("file_path", input_file_name())
        df_spark = parse_logs_to_df(logs_df)
        df = df_spark.toPandas()
        spark.stop()

    df = df.sort_values(["cluster_id", "start_time"])
    df["app_number"] = df.groupby("cluster_id").cumcount() + 1
    df["app_number"] = df["app_number"].apply(lambda x: f"{x:04d}")

    timeline_path = os.path.join(args.output_dir, "problem2_timeline.csv")
    df[["cluster_id", "application_id", "app_number", "start_time", "end_time"]].to_csv(timeline_path, index=False)
    print(f"[INFO] Wrote {timeline_path}")

    cluster_summary = df.groupby("cluster_id").agg(
        num_applications=("application_id", "count"),
        cluster_first_app=("start_time", "min"),
        cluster_last_app=("end_time", "max")
    ).reset_index()
    cluster_summary_path = os.path.join(args.output_dir, "problem2_cluster_summary.csv")
    cluster_summary.to_csv(cluster_summary_path, index=False)
    print(f"[INFO] Wrote {cluster_summary_path}")

    total_clusters = df["cluster_id"].nunique()
    total_apps = len(df)
    avg_apps = df.groupby("cluster_id")["application_id"].count().mean()
    top_clusters = cluster_summary.sort_values("num_applications", ascending=False)

    stats_lines = [
        f"Total unique clusters: {total_clusters}",
        f"Total applications: {total_apps}",
        f"Average applications per cluster: {avg_apps:.2f}\n",
        "Most heavily used clusters:"
    ]
    for _, row in top_clusters.iterrows():
        stats_lines.append(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications")

    stats_path = os.path.join(args.output_dir, "problem2_stats.txt")
    with open(stats_path, "w") as f:
        f.write("\n".join(stats_lines))
    print(f"[INFO] Wrote {stats_path}")

    # Visualization: Bar chart of apps per cluster
    plt.figure(figsize=(8,6))
    sns.barplot(x="cluster_id", y="num_applications", data=cluster_summary, palette="tab10")
    plt.title("Number of Applications per Cluster")
    plt.ylabel("Applications")
    plt.xlabel("Cluster ID")
    for idx, row in cluster_summary.iterrows():
        plt.text(idx, row["num_applications"] + 0.5, str(row["num_applications"]), ha="center")
    bar_chart_path = os.path.join(args.output_dir, "problem2_bar_chart.png")
    plt.tight_layout()
    plt.savefig(bar_chart_path)
    plt.close()
    print(f"[INFO] Wrote {bar_chart_path}")

    # Visualization: Density plot of job durations for largest cluster
    largest_cluster_id = top_clusters.iloc[0]["cluster_id"]
    df_largest = df[df["cluster_id"] == largest_cluster_id].copy()
    df_largest["duration_sec"] = (df_largest["end_time"] - df_largest["start_time"]).dt.total_seconds()

    plt.figure(figsize=(8,6))
    sns.histplot(df_largest["duration_sec"], kde=True, log_scale=True, bins=30)
    plt.title(f"Job Duration Distribution for Cluster {largest_cluster_id} (n={len(df_largest)})")
    plt.xlabel("Duration (seconds, log scale)")
    plt.ylabel("Count")
    density_plot_path = os.path.join(args.output_dir, "problem2_density_plot.png")
    plt.tight_layout()
    plt.savefig(density_plot_path)
    plt.close()
    print(f"[INFO] Wrote {density_plot_path}")

    print("[INFO] Done!")

if __name__ == "__main__":
    main()

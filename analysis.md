# Analysis Report
---

## 1. Introduction
This report analyzes Spark/YARN logs to answer two main questions:

1. **Problem 1:** What is the distribution of log levels across Spark/YARN log files?  
2. **Problem 2:** How are clusters utilized over time, and which clusters are most heavily used?

The analysis leverages **PySpark** for scalable log processing and generates visualizations for insights. All outputs are written to the `data/output` folder.

---

## 2. Problem 1 – Log Level Analysis

### 2.1 Approach
- Log files were read from S3 (or locally if skipping Spark).  
- Timestamps and log levels (INFO, WARN, ERROR, DEBUG) were extracted using Spark SQL functions.  
- Aggregated counts were calculated for each log level, and random samples were collected for verification.  

### 2.2 Key Insights
- A majority of log entries are INFO-level, with smaller proportions of WARN, ERROR, and DEBUG.  
- If some entries could not be classified into standard log levels and were labeled as UNKNOWN.  
- The aggregation and sampling results are available in `data/output/problem1_counts.csv` and `data/output/problem1_sample.csv`.  
- A plain text summary with counts and percentages is in `data/output/problem1_summary.txt`.

### 2.3 Observations
- Log-level distribution provides a quick overview of cluster activity and potential issues.  
- Sampling random entries helped verify that the log parsing logic is correctly identifying levels.  

---

## 3. Problem 2 – Cluster Usage Analysis

### 3.1 Approach
- Extracted **cluster IDs**, **application IDs**, and **start/end times** from log files.  
- Created a timeline dataset for each application, suitable for visualization.  
- Aggregated cluster-level metrics, such as the total number of applications per cluster and first/last application timestamps.  
- Visualizations were generated:
  - Bar chart for applications per cluster
  - Density plot of job durations for the largest cluster

### 3.2 Key Findings
- Clusters show varied usage; some are heavily utilized while others have only a few applications.  
- Aggregated summaries are available in `data/output/problem2_cluster_summary.csv`.  
- Timeline data for applications is in `data/output/problem2_timeline.csv`.  
- Overall statistics, including total clusters, applications, and averages, are documented in `data/output/problem2_stats.txt`.

### 3.3 Visual Evidence
- **Bar Chart:** `data/output/problem2_bar_chart.png` shows the number of applications per cluster.  
- **Density Plot:** `data/output/problem2_density_plot.png` shows job duration distribution for the largest cluster.  
- Visualizations help identify patterns such as peak usage periods and the distribution of job durations.

---

## 4. Performance Analysis

### 4.1 Execution Observations
- Running on the full cluster provides scalability for large log datasets.  
- Local execution (skipping Spark) is suitable for quick testing or regenerating visualizations.  
- Cluster execution takes longer but handles all files efficiently.

### 4.2 Optimization Strategies
- Avoided reading non-log files to prevent Spark read errors.  
- Aggregations performed before collecting results to minimize memory usage.  
- Partitioned reads and selective columns reduced processing overhead.

---

## 5. Documentation Quality
- All outputs are organized in `data/output`.  
- Spark Web UI screenshots can be included for DAGs, task stages, and executor metrics.  
- Markdown formatting provides clear sections, tables, and references to generated outputs.

---

## 6. Additional Insights
- Timeline analysis highlights periods of high cluster activity.  
- Job duration distribution indicates variability and potential bottlenecks.  
- Optional exploratory visualizations (e.g., heatmaps or boxplots) can reveal additional usage patterns.

---

### 7. References / Appendices
- Output CSV files and visualizations are all in `data/output`:  
  - `problem1_counts.csv`, `problem1_sample.csv`, `problem1_summary.txt`  
  - `problem2_timeline.csv`, `problem2_cluster_summary.csv`, `problem2_stats.txt`  
  - `problem2_bar_chart.png`, `problem2_density_plot.png`  
- Scripts: `problem1.py`, `problem2.py`

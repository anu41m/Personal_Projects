# Personal Projects - Data Engineering

This repository contains two of my personal projects that demonstrate my skills in data engineering, ETL processes, automation, and data analysis. The projects are as follows:

1. **Daily Expense Tracker**  
2. **Stock Analysis Tool**

---

## 1. Daily Expense Tracker

### Overview
The Daily Expense Tracker is a personal project that automates the process of tracking daily expenses. It extracts data from an Excel file, performs ETL (Extract, Transform, Load) transformations, and stores the processed data in Delta and CSV formats. The entire workflow is automated using Apache Airflow.

### Technologies Used
- **Docker**: To containerize and run the Spark cluster.
- **PySpark**: For ETL data processing.
- **Airflow**: For workflow orchestration and automation.
- **Delta Lake**: For structured data storage.
- **CSV**: For lightweight file storage.

### Project Structure
- `notebooks/`: Contains Jupyter notebooks for data extraction and processing.
- `dag_spark_cluster.py`: An Apache Airflow DAG to manage the workflow.
- `data/`: The folder where processed data (Delta and CSV files) will be stored.

### Setup Instructions

1. Clone the repository:
### Key Additions:
- run the `Cluster.sh` and `tr_airflow.sh` bash scripts for starting the Spark and Airflow containers locally.
- A note mentioning that the setup is done specifically for the **MacOS** platform and adjustments may be needed for data paths.

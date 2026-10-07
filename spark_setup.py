"""
aws-jobs-pipeline - shared Spark setup
======================================

Centralises the Windows-specific PySpark fixes (JAVA_HOME, PYSPARK_PYTHON,
HADOOP_HOME) and the SparkSession builder, so each stage stays focused on data
logic instead of repeating boilerplate.

In the CLOUD version (AWS Glue / Databricks) none of this is needed: the managed
runtime provides Spark, Java, and the Python workers. This file exists only so
the SAME pipeline logic runs locally on Windows for development and learning.
"""
from __future__ import annotations

import os
import sys


def _configure_env() -> None:
    # Spark runs on the JVM -> needs a JDK via JAVA_HOME. Point at JDK 17 if present.
    jdk = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
    if os.path.isdir(jdk):
        os.environ["JAVA_HOME"] = jdk
        os.environ["PATH"] = os.path.join(jdk, "bin") + os.pathsep + os.environ.get("PATH", "")

    # On Windows `python` can resolve to the Store stub; pin Spark workers + driver
    # to THIS interpreter so they start reliably.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # HADOOP_HOME + winutils let Spark write files on Windows. If a local hadoop/
    # folder (with bin/winutils.exe) exists, use it.
    hadoop = os.path.join(os.path.dirname(__file__), "hadoop")
    if os.path.isdir(os.path.join(hadoop, "bin")):
        os.environ["HADOOP_HOME"] = hadoop
        os.environ["PATH"] = os.path.join(hadoop, "bin") + os.pathsep + os.environ.get("PATH", "")


def get_spark(app_name: str):
    """Return a configured local SparkSession. Call once per stage."""
    _configure_env()
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(app_name)
        # local[*] = use all cores as worker threads. On AWS Glue/EMR this is
        # replaced by the cluster manager; the code below does not change.
        .master("local[*]")
        # Tiny data -> few shuffle partitions (default 200 is wasteful here).
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# Paths used across stages. In the cloud these become s3:// URIs.
BASE = os.path.dirname(__file__)
RAW_PATH = os.path.join(BASE, "data", "raw", "postings.jsonl")
BRONZE_PATH = os.path.join(BASE, "data", "bronze")
SILVER_PATH = os.path.join(BASE, "data", "silver")
GOLD_PATH = os.path.join(BASE, "data", "gold")

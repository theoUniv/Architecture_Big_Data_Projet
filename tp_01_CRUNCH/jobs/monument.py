#!/usr/bin/env python3
import sys
import time
from pathlib import Path

from pyspark.sql import functions as F

from lib import build_spark_session, ensure_output_dir, append_run_manifest, load_raw_df


if __name__ == "__main__":
    month = sys.argv[1]
    title = sys.argv[2] if len(sys.argv) > 2 else ""
    start = time.time()
    spark = build_spark_session("monument", parallelism=2)
    df = load_raw_df(spark, month)
    df = df.filter(F.col("title") == title)
    df = df.groupBy("title").sum("views").withColumnRenamed("sum(views)", "views")
    out_dir = ensure_output_dir("monument", time.strftime("%Y%m%d%H%M%S", time.gmtime()), 2)
    df.write.mode("overwrite").csv(str(out_dir / "result"), header=True)
    rows_in = df.count()
    rows_out = df.count()
    append_run_manifest("monument", month, 2, int(time.time() - start), rows_in, rows_out, title)
    spark.stop()

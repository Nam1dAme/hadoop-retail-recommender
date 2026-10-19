#!/usr/bin/env python3
"""Python 3.12 / Spark 3.5.7 / Java 8 fit-save-load smoke, using a tiny fixture."""

import json
import math
from pathlib import Path
import sys
import tempfile


def main():
    try:
        import pyspark
        from pyspark.ml.recommendation import ALS, ALSModel
        from pyspark.sql import SparkSession
    except ImportError:
        print("PySpark is not installed in this interpreter.", file=sys.stderr)
        return 3
    if sys.version_info[:2] != (3, 12) or pyspark.__version__ != "3.5.7":
        print("Expected Python 3.12 and PySpark 3.5.7.", file=sys.stderr)
        return 3
    runtime_dir = Path(__file__).resolve().parents[1] / "var/tmp"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.master("local[2]").appName("retail-als-environment-smoke")
             .config("spark.driver.memory", "2g").config("spark.sql.shuffle.partitions", "4")
             .config("spark.driver.bindAddress", "127.0.0.1")
             # This fixture tests local persistence independently of the HDFS service.
             .config("spark.hadoop.fs.defaultFS", "file:///")
             .config("spark.local.dir", str(runtime_dir)).getOrCreate())
    try:
        spark.sparkContext.setLogLevel("WARN")
        java_version = spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
        assert java_version.startswith("1.8."), java_version
        default_fs = spark.sparkContext._jsc.hadoopConfiguration().get("fs.defaultFS")
        assert default_fs == "file:///", default_fs
        frame = spark.createDataFrame([(0, 0, 1.0), (0, 1, 2.0), (1, 1, 1.0), (1, 2, 3.0), (2, 0, 2.0), (2, 2, 1.0)], "user int, item int, rating float")
        model = ALS(rank=16, maxIter=2, implicitPrefs=True, nonnegative=True, alpha=10.0, regParam=0.1, seed=42, numUserBlocks=2, numItemBlocks=2, coldStartStrategy="drop").fit(frame)
        before = sorted((r.user, r.item, float(r.prediction)) for r in model.transform(frame).collect())
        with tempfile.TemporaryDirectory(prefix="als-smoke-", dir=runtime_dir) as temporary:
            model_path = (Path(temporary) / "spark_model").resolve().as_uri()
            model.save(model_path)
            loaded = ALSModel.load(model_path)
            after = sorted((r.user, r.item, float(r.prediction)) for r in loaded.transform(frame).collect())
        assert len(before) == len(after) == 6
        assert all(a[:2] == b[:2] and math.isfinite(a[2]) and math.isfinite(b[2]) and abs(a[2] - b[2]) <= 1e-6 for a, b in zip(before, after))
        print(json.dumps({"status": "PASS", "fixture": True, "spark": pyspark.__version__, "java": java_version, "python": sys.version.split()[0], "filesystem": default_fs, "fit_save_load": True, "predictions": len(after)}))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

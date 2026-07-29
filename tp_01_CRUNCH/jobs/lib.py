import csv
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = Path(os.environ.get("SPARK_RAW_ROOT", str(DEFAULT_PROJECT_ROOT / "data" / "raw")))
OUT_ROOT = Path(os.environ.get("SPARK_OUTPUT_ROOT", str(DEFAULT_PROJECT_ROOT / "data" / "out")))
RUNS_CSV = OUT_ROOT / "runs.csv"

PROJECTS = {"fr", "fr.m"}
SPECIAL_PREFIXES = (
    "Special:",
    "Wikipédia:",
    "Catégorie:",
    "Discussion:",
    "Fichier:",
    "Media:",
    "Aide:",
    "Portail:",
    "Projet:",
)


def build_spark_session(app_name: str, parallelism: Optional[int] = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", max(200, parallelism or 2))
    )
    if parallelism:
        builder.config("spark.default.parallelism", parallelism)
    return builder.getOrCreate()


def list_input_files(month: str) -> List[Path]:
    month_dir = RAW_ROOT / month
    if not month_dir.exists():
        raise FileNotFoundError(f"Dossier introuvable : {month_dir}")
    return sorted(month_dir.glob("*.gz"))


def parse_pageviews_from_path(path: Path):
    match = re.search(r"pageviews-(\d{8})-(\d{2})0000\.gz$", path.name)
    if not match:
        raise ValueError(f"Nom de fichier non reconnu : {path.name}")
    day = match.group(1)
    hour = match.group(2)
    dt = datetime.strptime(f"{day}-{hour}", "%Y%m%d-%H")
    return dt


def load_raw_df(spark: SparkSession, month: str):
    files = list_input_files(month)
    if not files:
        raise FileNotFoundError(f"Aucun fichier trouvé dans {RAW_ROOT / month}")

    schema = ["project", "title", "views", "bytes"]
    file_paths = [str(path) for path in files]
    df = spark.read.text(file_paths)
    df = df.selectExpr("value as line")
    df = df.filter(df.line.isNotNull())
    df = df.selectExpr("split(line, ' ') as parts")
    df = df.filter(F.size("parts") >= 4)
    df = df.select(
        F.col("parts")[0].alias("project"),
        F.col("parts")[1].alias("title"),
        F.col("parts")[2].cast("int").alias("views"),
        F.col("parts")[3].alias("bytes"),
    )
    df = df.filter(F.col("project").isin(list(PROJECTS)))
    df = df.filter(~F.col("title").startswith("Special:"))
    df = df.filter(~F.col("title").contains("Wikipédia:"))
    df = df.filter(~F.col("title").contains("Catégorie:"))
    df = df.filter(~F.col("title").contains("Discussion:"))
    df = df.filter(F.col("title") != "-")
    df = df.filter(F.col("title") != "Accueil")
    return df


def ensure_output_dir(job_name: str, timestamp: str, parallelism: int) -> Path:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    out_dir = OUT_ROOT / f"{job_name}_{timestamp}_p{parallelism}"
    if out_dir.exists():
        raise FileExistsError(f"Répertoire déjà existant : {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def append_run_manifest(job_name: str, month: str, parallelism: int, duration_s: int, rows_in: int, rows_out: int, title: Optional[str] = None) -> None:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    RUNS_CSV.touch(exist_ok=True)

    run_id = f"{job_name}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    if title:
        run_id = f"{run_id}_{title.replace(' ', '_')}"

    row = {
        "run_id": run_id,
        "ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "job": job_name,
        "month": month,
        "parallelism": parallelism,
        "duration_s": duration_s,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "title": title or "",
    }

    write_header = not RUNS_CSV.exists() or RUNS_CSV.stat().st_size == 0
    with RUNS_CSV.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["run_id", "ts_utc", "job", "month", "parallelism", "duration_s", "rows_in", "rows_out", "title"])
        if write_header:
            writer.writeheader()
        writer.writerow(row)

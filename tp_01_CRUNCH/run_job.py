#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def get_compose_command():
    for cmd in (["docker-compose"], ["docker", "compose"]):
        try:
            subprocess.run(cmd + ["version"], cwd=ROOT, check=True, capture_output=True, text=True)
            return cmd
        except Exception:
            continue
    raise RuntimeError("Docker Compose n’est pas disponible sur cette machine")


def check_stack():
    print("Vérification de la stack")
    try:
        compose_cmd = get_compose_command()
        subprocess.run(compose_cmd + ["ps"], cwd=ROOT, check=True, capture_output=True, text=True)
        print(f"OK: {' '.join(compose_cmd)} ps")
    except Exception as exc:
        print(f"ERR: {exc}")


def run_job(job_name: str, month: str, title: str | None = None, compose_file: str = "docker-compose.1w.yml"):
    compose_cmd = get_compose_command()
    spark_submit = compose_cmd + ["-f", compose_file, "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "--deploy-mode", "client"]
    if job_name == "top100":
        spark_submit.append("/opt/spark/jobs/top100.py")
        spark_submit.extend([month])
    elif job_name == "monument":
        spark_submit.append("/opt/spark/jobs/monument.py")
        spark_submit.extend([month, title or ""])
    else:
        raise ValueError(f"Job inconnu: {job_name}")

    print("Commande:", " ".join(spark_submit))
    result = subprocess.run(spark_submit, cwd=ROOT, text=True)
    return result.returncode


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("job", nargs="?", help="Nom du job: top100 ou monument")
    parser.add_argument("month", nargs="?", help="Mois à traiter")
    parser.add_argument("title", nargs="?", help="Titre exact pour monument")
    parser.add_argument("--compose-file", default="docker-compose.1w.yml")
    parser.add_argument("--check", action="store_true", help="Vérifie l’état de la stack")
    args = parser.parse_args()

    if args.check:
        check_stack()
        sys.exit(0)

    if not args.job or not args.month:
        parser.error("Usage: run_job.py <top100|monument> <month> [title]")

    sys.exit(run_job(args.job, args.month, args.title, args.compose_file))

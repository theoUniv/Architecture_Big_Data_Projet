# TP Crunch Pipeline Spark

Ce projet implémente un pipeline batch PySpark basé sur le sujet fourni.

## Structure
- jobs/: scripts PySpark et bibliothèque commune
- data/raw/: données brutes (fichiers pageviews à télécharger ou à placer ici)
- data/out/: sorties et manifestes de runs
- docker-compose*.yml: variantes de cluster Spark avec Prometheus/Grafana/cAdvisor
- prometheus/: configuration Prometheus
- grafana/: provisionnement de datasource et dashboard

## Utilisation
1. Placer les données brutes dans data/raw/2026-06/.
2. Démarrer la stack souhaitée : docker compose -f docker-compose.1w.yml up -d
3. Exécuter un job : python3 run_job.py top100 2026-06
4. Vérifier l’état : python3 run_job.py check

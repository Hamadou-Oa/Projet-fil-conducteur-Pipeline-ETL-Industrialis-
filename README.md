# Pipeline ETL Industrialisé avec Spark, Airflow et MinIO

## 1. Contexte du projet

Ce projet est un projet de synthèse réalisé dans le cadre de la validation de la specialisation en Data Engineering .
Il vise à concevoir et implémenter un pipeline ETL (Extract, Transform, Load) complet, automatisé et industrialisé,
en appliquant les concepts et outils étudiés en Data Engineering.

Le pipeline permet la collecte automatique de données depuis plusieurs sources,
leur transformation distribuée, et leur stockage dans une architecture moderne de type Data Lakehouse.

---

## 2. Objectifs du projet

Les objectifs principaux du projet sont :

- Collecter automatiquement des données depuis plusieurs sources hétérogènes
- Mettre en œuvre des transformations distribuées avec Apache Spark
- Stocker les données dans un object storage S3-compatible (MinIO)
- Orchestrer l’ensemble du pipeline avec Apache Airflow
- Industrialiser le projet via Docker
- Appliquer les bonnes pratiques de qualité logicielle (structure, tests, documentation)

---

## 3. Architecture globale du pipeline

Le pipeline suit l’architecture ETL classique :

1. **Extraction**
   - Site web (scraping)
   - Fichier CSV
   - Base de données SQL (PostgreSQL)

2. **Transformation**
   - Nettoyage et normalisation des données
   - Traitements distribués avec Apache Spark
   - Fusion des données multi-sources

3. **Chargement**
   - Stockage des données au format Parquet
   - Data Lakehouse sur MinIO
   - Partitionnement des données par source

4. **Orchestration**
   - Pipeline automatisé avec Apache Airflow
   - Gestion des dépendances et reprises sur échec

---

## 4. Technologies utilisées

- **Python 3**
- **Apache Spark** (traitements distribués)
- **Apache Airflow** (orchestration)
- **MinIO** (object storage S3-compatible)
- **PostgreSQL** (source SQL)
- **Docker & Docker Compose** (conteneurisation)
- **Git & GitHub** (versionnement)

---

## 5. Organisation du projet


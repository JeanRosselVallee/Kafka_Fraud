
# Documentation du projet "Détection d'anomalies de flux financiers avec Apache Kafka"

Ce projet vise à ... utilisant Apache Kafka.

**Sommaire**
---

- [1. Contexte](#1-contexte)
- [2. Cahier des charges](#2-cahier-des-charges)
- [3. Solution Technique](#3-solution-technique)
- [4. Guide de l'Administrateur](#4-guide-de-ladministrateur)
- [5. Guide du Développeur](#5-guide-du-développeur)
- [6. Guide du Data Scientist](#6-guide-du-data-scientist)
- [7. Annexe: Glossaire & Acronymes](#7-annexe-glossaire-et-acronymes)

---

## 1. Contexte

### 1.1 Description du projet

Une grande banque en ligne souhaite améliorer sa réactivité face aux transactions frauduleuses afin de servir au mieux ses clients. 

Dans son système, l'entreprise enregistre toutes les transactions dans une base de données. 

Afin d'aider les Data Scientists à calibrer un modèle de Machine Learning qui puisse identifier les transactions frauduleuses, l'entreprise utilise des robots qui ajoutent artificiellement des fraudes dans les données.

Le présent projet consiste à mettre en place un Consumer Kafka pour analyser les transactions en temps réel.

### 1.2 Objectifs Business

- **Détecter** en temps réel les transactions frauduleuses qui pourraient survenir sur les comptes de ses clients.
- **Stocker** en base toutes les transactions financières en temps réel.

---

## 2. Cahier des charges

### 2.1 Objectifs fonctionnels

- **Ingestion de données** : Utiliser Kafka 
- **Consumer** : analyse les transactions en temps réel et remonte les frauduleuses.

### 2.2 Besoins Métier & Règles de Gestion

- **Charge moyenne maximale** : l'algorithme de détection doit être en mesure de pouvoir supporter jusqu'à une moyenne de 500 transactions par seconde.
- **Continuous Integration** : le code doit être déployé et s'exécuter en continu sur le Cloud (machine virtuelle ou serverless type Cloud Run sur GCP).
-- **Producer** : fourni en Python

### 2.3 Description des données

**Sources** : 
1. fichier Parquet de données de transactions financières avec fraudes (225 Mo)
2. le Producer Kafka envoi les transactions sur un topic

**Schéma des fichiers** :

| Colonne | Type | Description |
|---------|------|-------------|
| `step` | string (UTC) | Référence des données générées |
| `type` | string | Type de transaction: débit, crédit, transfer, retrait, etc. |
| `amount` | string | Montant de transaction |
| `name[Orig\|Dest]` | string | nom du compte Source/Destination |
| `[old\|new]balance[Org\|Dest]` | string | solde Source/Destination avant/après transaction |
| `isFraud` | string | 1 si fraude, 0 sinon |

**Volume** 
- pour le fichier Parquet, 6 millions de transactions.
- pour le flux Kafka, 500 transactions par seconde.

### 2.4 Étapes du projet

1. **Analyse de transactions par Consumer** - Créer un Consumer qui reçoit les messages envoyés par le Producer sur le topic "transactions" afin d'obtenir des informations sur l'ensemble des transactions financières du jeu de données, notamment des statistiques sur des périodes de 30 secondes (fenêtre glissante/rafraîchissement toutes les secondes):
- nombre de transactions réalisées au cours des 30 dernières secondes.
- nombre de transactions frauduleuses au cours des 30 dernières secondes.
- montant total des transactions (frauduleuses et non frauduleuses).
- la part que représente le montant des transactions frauduleuses sur le montant total.

2. **Paramétrage et écriture cible** – Paramétrer le script Spark et écrire la table de sortie vers un système cible. En ayant pris soin de stocker les données sur un système de stockage d'objets (AWS S3, GCS ou HDFS), le script doit être adapté afin de pouvoir prendre en compte les paramètres DESTINATION, DATE_START et DATE_END lors du lancement du job Spark. Le script doit également être adapté pour enregistrer la table de sortie au format CSV au chemin DESTINATION spécifié en paramètre.

3. **Test en conditions réelles** – Tester le job Spark en conditions réelles. En lançant un cluster Hadoop dans le Cloud, tester le job Spark en l'exécutant avec DATE_START et DATE_END sur deux mois différents et avec un écart de 2 semaines.

4. **Documentation et livraison** – Écrire un guide (fichier README) expliquant les codes du projet et permettant de prendre en main rapidement le projet à partir du dépôt Git

### 2.5 Environnement de développement : Sandbox Dataproc

L'environnement est une sandbox Cloud GCP avec les composants suivants pour la soumission du job Spark avec les paramètres souhaités.

- Cluster Dataproc nommé `main-cluster`, région `us-central1`, avec Spark et Hadoop préinstallés.
- Bucket GCS pour stocker les fichiers CSV des sorties transformées.
- Service de transfert Storage Transfer Service (STS) pour copier des fichiers depuis des URLs publiques vers un bucket GCS.
- Contraintes temporelles – La sandbox a une durée de vie limitée à 4h. Tous les résultats sont donc effacés après ce délai.

# 3. Solution Technique

<img src="../docs/infographic_workflow_river.png" width="90%">


## 3.1 Outils

| Catégorie | Outil / Service | Usage |
|-----------|----------------|-------|
| Langage | Python 3.14 | Développement des scripts ETL |
| Framework Big Data | PySpark (via pyspark) | Traitement distribué sur cluster |
| Orchestration | Bash (`run_all_scripts.sh`) | Enchaînement des étapes (setup, transfert, ETL) |
| Cloud Provider | Google Cloud Platform (GCP) | Infrastructure et services |
| Stockage objet | Google Cloud Storage (GCS) | Bucket pour données brutes, scripts et sorties |
| Calcul distribué | Google Cloud Dataproc (Spark / Hadoop) | Cluster éphémère pour exécution du job Spark |
| Transfert de données | Storage Transfer Service (STS) | Copie depuis URLs publiques (S3) vers GCS |
| Authentification | gcloud CLI + Application Default Credentials (ADC) | Accès sécurisé aux APIs GCP |
| Tests | Pytest | Validation unitaire et intégration |
| Gestion de configuration | configparser (INI) + argparse | Paramètres centralisés et override en ligne de commande |
| Journalisation | logging (Python) | Logs coloriés console + fichier |


## 3.2 Arborescence du Projet
```bash
.
├── src/
│   ├── config.ini
│   ├── job_spark.py        # Spark job
│   ├── job_transfer.py     # AWS → GCS transfer utility
│   ├── lib_common.py       # Shared utilities
│   ├── nbook_prototype.ipynb # Required with R/W to local storage
│   └── nbook_cloud.ipynb   # Optional with R/W to GCS
├── log/
│   ├── run_all_scripts_<timestamp>.log
│   ├── job_transfer.log
│   └── test_etl_job.log
├── tests/
│   ├── mock_data.csv       # Sample input for tests
│   ├── expected_output.csv # Reference output
│   └── test_etl_job.py     # Pytest suite
├── pytest.ini              # Pytest configuration
├── setup_dev_env.sh        # Dev environment setup
├── requirements.txt        # Python dependencies
├── run_all_scripts.sh      # Convenience runner
├── setup_data_services.sh  # GCS/S3 bucket provisioning
└── README.md
```

**Rôles des fichiers clés :**
- `lib_common.py` – classes et fonctions partagées (configuration, logging, accès GCS, génération des noms de fichiers mensuels).
- `job_transfer.py` – transfert des fichiers bruts depuis S3 (URLs publiques) vers le bucket GCS via STS.
- `job_spark.py` – job ETL principal : lecture CSV, nettoyage, feature engineering, écriture CSV.
- `config.ini` – paramètres de stockage, Spark (seuils), logging et débogage.
- `run_all_scripts.sh` – orchestrateur : setup, transfert, soumission Spark avec paramètres.
- `tests/` – jeux de données mock et des réultats attendus + suite Pytest.
- `requirements.txt` – dépendances Python (pyspark, google-cloud-storage, etc.)

## 3.3 Architecture

### 3.3.1 Diagramme de flux de données
```mermaid
%%{init: {'flowchart': {'rankSpacing': 30}}}%%
flowchart TD
  %% Define styles (couleur d'arrière-plan des blocs)
  classDef lightblueStyle fill:#e3f2fd,stroke:#0d47a1,stroke-width:2px;
  classDef greenStyle fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px;
  classDef yellowStyle fill:#fff9c4,stroke:#f9a825,stroke-width:2px;
  classDef orangeStyle fill:#ffcc80,stroke:#e65100,stroke-width:2px;
  classDef compact font-size:10px

  %% Nœuds / flèches
  A["Data Scientist (lance le script avec paramètres)"]:::compact --> B[run_all_scripts.sh];
  B:::compact --> C[setup_env_dev.sh]:::compact;
  B:::compact --> D[setup_data_services.sh]:::compact;
  B:::compact --> E[job_transfer.py];
  E:::compact --> F[S3 URLs];
  F:::compact --> G[Storage Transfer Service];
  G:::compact --> H["GCS Bucket (data/raw/)"];
  B:::compact --> I[job_spark.py];
  H:::compact --> I;
  I:::compact --> J["Dataproc Cluster (Spark/Hadoop)"]:::compact;
  J:::compact --> K["GCS Bucket (data/processed/)"]:::compact;
  K:::compact --> L["Data Scientist (récupère CSV)"]:::compact;

  %% Associer chaque nœud à une couleur stylisée
  class A greenStyle;
  class B orangeStyle;
  class C yellowStyle;
  class D yellowStyle;
  class E yellowStyle;
  class F lightblueStyle;
  class G lightblueStyle;
  class H lightblueStyle;
  class I yellowStyle;
  class J lightblueStyle;
  class K lightblueStyle;
  class L greenStyle;
  ```

Flux détaillé :

1. Injection initiale – Les fichiers CSV mensuels sont hébergés sur AWS S3 (URLs publiques).
2. Transfert – job_transfer.py construit une liste d'URLs, génère un fichier TSV, crée un job STS qui copie les fichiers vers le bucket GCS dans data/raw/.
3. ETL – job_spark.py est soumis à Dataproc. Il lit les fichiers mensuels correspondant à la plage DATE_START – DATE_END, applique les transformations (nettoyage, agrégations par session/produit, fenêtres historiques) et écrit le résultat en CSV dans DESTINATION.
4. Récupération – Le Data Scientist télécharge le fichier CSV depuis le chemin indiqué.


### 3.3.2 Diagramme de séquence
```mermaid
sequenceDiagram
    actor DS as Data Scientist
    participant Orch as run_all_scripts.sh
    participant STS as Storage Transfer Service
    participant GCS as Cloud Storage
    participant DP as Dataproc Cluster
    DS->>Orch: ./run_all_scripts.sh -f all -s ... -e ... -d ...
    Orch->>Orch: Setup ENV (venv, deps)
    Orch->>STS: Créer job transfert (TSV URLs)
    STS->>GCS: Copier data/raw/YYYY-Mon.csv
    STS-->>Orch: Succès transfert
    Orch->>DP: gcloud dataproc jobs submit pyspark job_spark.py
    DP->>GCS: Lire data/raw/
    DP->>DP: ETL (Clean, Agg, Window)
    DP->>GCS: Écrire data/processed/.../part-*.csv + _SUCCESS
    DP-->>Orch: Job Succeeded
    Orch-->>DS: Chemin sortie GCS + Logs
```

Le diagramme illustre le flux d’exécution typique du pipeline Blent Spark lorsqu’un Data Scientist lance le job depuis son poste de travail. 

Ce flux montre comment les scripts d’orchestration, le service de transfert et le cluster Spark interagissent de façon séquentielle pour passer de données brutes depuis de URL à une table de sortie exploitable dans GCS.


1. Initiation
    - Le Data Scientist exécute le script d’orchestration `run_all_scripts.sh` en spécifiant les paramètres de la période à traiter (DATE_START, DATE_END) et le chemin de destination GCS.

2. Pré‑configuration
    - `run_all_scripts.sh` déclenche d’abord setup_env_dev.sh, qui installe l’environnement Python (venv) et les dépendances listées dans requirements.txt.
    - Ensuite, il lance setup_data_services.sh, qui s’occupe de l’authentification GCP (gcloud auth), de la création du bucket GCS (si besoin) et de l’attribution des rôles IAM nécessaires (Storage Admin, Service Account Admin, etc.).

3. Transfert des données brutes
    - Le script invoque job_transfer.py.
    - Ce dernier récupère la liste des URLs publiques contenant les fichiers CSV mensuels.
    - Un fichier TSV contenant les URLs et leurs tailles est généré puis uploadé dans le bucket GCS, dans le répertoire data/raw/.
    - Le Storage Transfer Service (STS) copie les fichiers depuis les URLs vers le bucket GCS, affichant une progression en pourcentage et en volume.

4. Exécution du job Spark
    - Après le transfert, `run_all_scripts.sh` soumet le job Spark via gcloud dataproc jobs submit pyspark job_spark.py.
    - Le cluster Dataproc (Spark/Hadoop) démarre, lit les fichiers CSV depuis le bucket data/raw/, applique le nettoyage, les agrégations et les fenêtres de calcul définies dans job_spark.py.
    - Le job écrit le résultat final sous forme de CSV partitionné (part-*.csv) dans le répertoire data/processed/ du bucket GCS, puis crée le fichier _SUCCESS pour indiquer la réussite.

5. Récupération des résultats
    - Une fois le job terminé, le Data Scientist utilise gcloud storage cp ou gcloud storage cat pour télécharger ou concaténer les fichiers CSV depuis le chemin GCS fourni.

## 3.4 Schéma de données

### 3.4.1 Entrée (fichier CSV brut)
Données envoyées par Producer 

| Colonne | Type | Description |
|---------|------|-------------|
| `step` | string (UTC) | Référence des données générées |
| `type` | string | Type de transaction: débit, crédit, transfer, retrait, etc. |
| `amount` | string | Montant de transaction |
| `name[Orig|Dest]` | string | nom du compte Source/Destination |
| `[old|new]balance[Org|Dest]` | string | solde Source/Destination avant/après transaction |
| `isFraud` | string | 1 si fraude, 0 sinon |

### 3.4.2 Sortie (CSV final – maille session × article)

| Colonne | Type | Description |
|---------|------|-------------|
| `product_id` | string | Identifiant produit |
| `category_code` | string | Code catégorie |
| `brand` | string | Marque |
| `price` | double | Prix unitaire |
| `user_session` | string | Session concernée |
| `purchased` | int (0/1) | Achat de l'article dans la session |
| `num_views_product` | int | Nombre de vues de l'article dans la session |
| `num_views_session` | int | Nombre d'articles distincts vus dans la session |
| `start_time` | string (HH:MM) | Heure de début de la session |
| `start_weekday` | string | Jour de la semaine (ex: Monday) |
| `duration` | long | Durée de la session (secondes) |
| `num_prev_sessions` | int | Nombre de sessions antérieures de l'utilisateur |
| `num_prev_product_views` | int | Nombre de vues antérieures de ce produit par l'utilisateur |


### 3.5 Implémentation des étapes du projet
Le projet a été implémenté en suivant les étapes détaillées dans la section [2.4 Étapes du projet](#24-étapes-du-projet)

1. **Prototypage sur échantillon** – Un notebook Jupyter [nbook_prototype.ipynb](../src/nbook_prototype.ipynb) a été réalisé pour concevoir pas à pas les transformations Spark sur un extrait de 150 Mo (échantillon d'Octobre 2019). Ce notebook a servi à valider les formules de calcul (agrégations, fenêtres, jointures) et à définir la structure finale de la table.

2. **Paramétrage et écriture cible** – Le script [job_spark.py](../src/job_spark.py) a été adapté pour lire les paramètres `DESTINATION`, `DATE_START`, `DATE_END` via `argparse`. La sortie est écrite en CSV dans le système cible (GCS ou HDFS). Le fichier `config.ini` centralise les valeurs par défaut (chemins, seuils de cache, format de date).

3. **Tests unitaires et d'intégration** – Le répertoire `tests/` contient :
   - [mock_data.csv](../tests/mock_data.csv) – un petit jeu de données synthétique.
   - [expected_output.csv](../tests/expected_output.csv) – la sortie de référence correspondante.
   - [test_etl_job.py](../tests/test_etl_job.py) – une suite Pytest qui valide les fonctions de nettoyage, d'agrégation et de calcul des caractéristiques sur le mock, ainsi que l'intégration du pipeline complet (extraction → transformation → chargement en mémoire). 
   
   Cf. [test_job_spark_20260724_2358.log](./test_job_spark_20260724_2358.log) qui montre le déroulement du test. 

4. **Test en conditions réelles** – Une connexion à un cluster Dataproc (Hadoop + Spark) a été établie via `setup_data_services.sh`. Le job a été exécuté avec des plages de deux mois pour valider les performances et la stabilité. Cf. [run_all_scripts_20260727_1439.log](./run_all_scripts_20260727_1439.log) pour la période 2019-12-01 → 2020-02-01.

5. **Documentation et livraison** – Un `README.md` complet décrit l'architecture, les prérequis, les commandes d'installation et d'exécution, ainsi que les résultats attendus.

## 3.6 Référencement des Données

### 3.6.1 Stockage Source (Injection Initiale)
- **Format** : CSV mensuel, nommé `YYYY-Mon.csv` (ex: `2019-Oct.csv`).
- **Localisation initiale** : URLs publiques AWS S3 (paramètre `SOURCE_URL_DIR` dans `config.ini`).
- **Exemple** : `https://blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/9c15cb/2019-Oct.csv`

### 3.6.2 DataFrames Spark (mémoire)
- **Raw DataFrame** : lecture directe des CSV avec `spark.read.csv(..., inferSchema=False)` – toutes les colonnes en `string`.
- **Cleaned DataFrame** : après filtrage des sessions incohérentes (plusieurs `user_id`).
- **Filtered DataFrame** : après application des filtres `DATE_START` et `DATE_END` sur `event_time`.
- **Feature DataFrames intermédiaires** :
  - `sdf_per_product` : agrégation par `(user_session, product_id)` – calcul de `purchased` et `num_views_product`.
  - `sdf_per_session` : agrégation par `user_session` – calcul de `num_views_session`, `start_time`, `weekday`, `duration`, `num_prev_sessions` (via fenêtre).
  - `sdf_features` : jointure entre les deux, puis ajout de `num_prev_product_views` (via fenêtre sur `(product_id, user_id)`).

### 3.6.3 Stockage Destination
- **Format** : CSV avec en-tête, écrit en mode `overwrite`.
- **Chemin** : paramètre `DESTINATION` (ex: `gs://bucket9/data/processed/run_20260714`).
- **Fichiers générés** : plusieurs `part-*.csv` (coalesce ou repartition selon volume).
- **Fichier de succès** : `_SUCCESS` présent si l'écriture est complète.

## 3.7 Matrice de traçabilité

| Exigence fonctionnelle | Implémentation technique | Fichier / Fonction | Test associé |
|------------------------|--------------------------|-------------------|--------------|
| Paramètres DATE_START, DATE_END, DESTINATION | argparse + config.ini | `job_spark.py` (parse_arguments) / `run_all_scripts.sh` | `test_etl_job.py` (mock avec dates) |
| Filtrage plage de dates | `filter(col("event_time") >= start & <= end)` | `transform()` dans `job_spark.py` | Vérification sur mock |
| Sortie par défaut HDFS si DESTINATION vide | Valeur par défaut dans argparse | `parse_arguments()` | Test avec `DESTINATION=None` |
| ** Nettoyage sessions multi-user | `groupBy("user_session").count().filter(count>1)` puis `left_anti` | `clean_data()` | `test_clean_data()` |
| Calcul `purchased` | `max(event_type == "purchase")` par groupe | `get_features_per_product()` | `test_purchased_flag()` |
| Calcul `num_views_product` | `sum(event_type == "view")` par groupe | `get_features_per_product()` | `test_num_views_product()` |
| Calcul `num_views_session` | `countDistinct(product_id)` filtré sur `view` | `get_features_per_session()` | `test_num_views_session()` |
| Calcul `duration` | `unix_timestamp(max) - unix_timestamp(min)` | `get_features_per_session()` | `test_duration()` |
| Calcul `start_time` / `start_weekday` | `date_format(min(event_time), "HH:mm")` et `"EEEE"` | `get_features_per_session()` | `test_start_metadata()` |
| Calcul `num_prev_sessions` | Fenêtre `Window.partitionBy("user_id").orderBy("temp_start_dt").rowsBetween(-∞, -1)` | `get_features_per_session()` | `test_num_prev_sessions()` |
| Calcul `num_prev_product_views` | Fenêtre sur `(product_id, user_id)` avec `sum(num_views_product)` avant session | `get_features_per_product_per_session()` | `test_num_prev_product_views()` |
| Écriture CSV | `df.write.mode("overwrite").csv(path, header=True)` | `load()` | Vérification présence `_SUCCESS` |
| ** Gestion volumétrie | Cache conditionnel (`persist`) + repartition (`repartition(optimal)`) | `transform()` et `get_optimal_nb_partitions()` | Test de performance sur 2 mois |
| ** Logging et monitoring | `logging` + `run_all_scripts.sh` redirige stdout/stderr vers log | `lib_common.py` / shell | Vérification logs |

**: Exigences ajoutées par nos choix d'implémentation

## 3.8 Choix d'amélioration

### 3.8.1 Performance – cache et partitionnement
- **Cache conditionnel** : Le DataFrame après filtrage est persisté (`sdf.persist()`) uniquement si son nombre de lignes est inférieur à `MAX_NB_CACHED_ROWS` (200 M). Ce seuil évite de saturer la mémoire des executors tout en optimisant les deux branches (agrégation produit et session) issues de la même source.
- **Repartitionnement optimisé** : Avant écriture, le DataFrame de sortie est réparti en `optimal_nb_partitions = max(10, sdf_count / 200_000)`. Cela équilibre la charge et évite les fichiers trop gros ou trop nombreux.
- **Lecture sans inferSchema** : Toutes les colonnes sont lues en `string` pour accélérer l'ingestion. Les conversions sont faites uniquement lorsque nécessaires (ex: `event_time`).
- **Utilisation de `spark.sql.legacy.timeParserPolicy=LEGACY`** : Permet d'accepter le suffixe "UTC" dans les timestamps sans erreur.

### 3.8.2 Robustesse
- **Validation des paramètres** : Vérification que `DATE_START < DATE_END` et que `DESTINATION` est bien un sous-chemin du bucket.
- **Gestion des erreurs** : Try/except dans `main()` avec `sys.exit(1)` en cas d'échec, et logging systématique de l'étape en cours.
- **Présence des fichiers source** : Avant de soumettre le job Spark, `job_transfer.py` vérifie que les URLs sont accessibles (HEAD request) et que les fichiers sont bien présents dans le bucket après transfert.
- **Surveillance du transfert STS** : Le script `monitor_transfer()` interroge périodiquement l'état du job STS et affiche la progression (pourcentage, volume transféré) jusqu'à complétion.
- **Gestion du cache** : Le DataFrame feature est mis en cache avec `cache()` avant de compter les lignes, évitant de relire les données.

### 3.8.3 Sécurité
- **Authentification Application Default Credentials (ADC)** : Les appels aux APIs GCP utilisent `google.auth.default()`, qui récupère les identifiants de l'utilisateur connecté via `gcloud auth application-default login`.
- **Gestion des secrets** : Aucun secret en clair dans le code. Les identifiants sont ceux de l'opérateur (IAM) ; le compte de service du STS est géré automatiquement par Google.
- **Accès limité au bucket** : Les politiques IAM sont appliquées via `setup_data_services.sh` (rôle `storage.objectAdmin` pour l'opérateur, `storage.admin` pour le compte STS).
- **Journalisation sécurisée** : Les logs ne contiennent pas de données sensibles (user_id, session_id sont des identifiants techniques).
- **Séparation des rôles** : Le transfert utilise un compte de service Master (géré par l'utilisateur) et un compte Worker (géré par Google) – les permissions sont strictement limitées aux opérations nécessaires.

# 4 Guide de l'Administrateur

## 4.1 Déploiement

### Étape 1 : Préparation des Infrastructures Cloud

Avant tout déploiement, l'administrateur doit s'assurer que les prérequis GCP sont satisfaits. Il doit disposer d'un projet GCP actif avec la facturation activée, et posséder les rôles IAM suivants : Storage Admin, Service Account Admin, et Dataproc Admin. Les APIs nécessaires (Dataproc, Cloud Storage, Storage Transfer Service, IAM) doivent être activées sur le projet via la console GCP ou la commande ci-dessous.

```bash
gcloud services enable dataproc.googleapis.com storage.googleapis.com storagetransfer.googleapis.com iam.googleapis.com
```

L'administrateur vérifie ensuite que le fichier config.ini contient les identifiants corrects pour le bucket (BUCKET_NAME) et la région (REGION). Si le nom du bucket est déjà utilisé, il devra en choisir un autre et mettre à jour la configuration. La connexion au cluster Dataproc se fera  automatiquement par le script `setup_data_services.sh`. Pour vérifier que les APIs sont bien activées, utiliser :

```bash
gcloud services list --enabled --filter="NAME:dataproc OR NAME:storage OR NAME:storagetransfer"
```

### Étape 2 : Clonage et Configuration logicielle

L'administrateur clone le dépôt Git contenant l'ensemble des sources sur la machine locale ou sur une VM de bastion. Il exécute ensuite le `script setup_env_dev.sh` qui installe automatiquement GCloud CLI (si absent), crée un environnement virtuel Python (venv_spark), et installe toutes les dépendances listées dans requirements.txt. Ce script est conçu pour être idempotent : il peut être réexécuté sans effet secondaire.

```bash
git clone <url-du-depot>
cd <nom-du-projet>
source ./setup_env_dev.sh
```

Une fois l'environnement prêt, l'administrateur vérifie que les chemins dans config.ini sont corrects, notamment LOG_DIR pour la journalisation locale et les valeurs par défaut des paramètres Spark. Il peut également ajuster le seuil MAX_NB_CACHED_ROWS en fonction de la mémoire disponible sur les exécuteurs Dataproc. Aucune modification du code source n'est normalement nécessaire, sauf pour des adaptations spécifiques (ex : changer le format de date ou ajouter une nouvelle colonne de sortie). Pour tester que l'environnement est opérationnel :

```bash
python3 -c "import pyspark, google.cloud.storage; print('OK')"
```

### Étape 3 : Gestion de rôles et permissions

L'administrateur doit s'assurer que l'utilisateur opérateur (celui qui exécute les scripts) possède les droits IAM appropriés. Le script `setup_data_services.sh` vérifie automatiquement que l'opérateur a le rôle roles/iam.serviceAccountAdmin pour créer le compte de service du Storage Transfer Service. Si ce n'est pas le cas, l'administrateur doit attribuer ce rôle via la console GCP ou via la commande suivante :

```bash
PROJECT_ID=$(gcloud projects list --filter='projectId:blent-sandbox-*' --format='value(projectId)' --limit=1)
OPERATOR_EMAIL=$(gcloud config get-value account)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="user:$OPERATOR_EMAIL" \
    --role="roles/iam.serviceAccountAdmin"
```

Le même script crée le bucket GCS et lui attribue les politiques d'accès : rôle storage.objectAdmin pour l'opérateur, et rôle storage.admin pour le compte de service du STS (identifié par project-{PROJECT_NUMBER}@storage-transfer-service.iam.gserviceaccount.com). Si l'administrateur souhaite restreindre davantage les accès, il peut ajuster ces rôles après exécution. Enfin, il doit s'assurer que le compte de service Dataproc (par défaut compute-engine) a au moins le rôle storage.objectAdmin sur le bucket pour lire les scripts et les données, et écrire les résultats.

```bash
BUCKET_NAME=$(grep "^BUCKET_NAME =" ./src/config.ini | cut -d" " -f 3)
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')
STS_AGENT="project-${PROJECT_NUMBER}@storage-transfer-service.iam.gserviceaccount.com"
gcloud storage buckets add-iam-policy-binding gs://$BUCKET_NAME \
    --member="serviceAccount:$STS_AGENT" \
    --role="roles/storage.admin"
```

#### Rôles requis pour le transfert de fichiers

|          Action              |Component |      Required Role         |    Token    | GCP Backend |
|------------------------------|----------|----------------------------|-------------|-------------|
| Create Bucket                | Operator | storage.admin              | Token       | GCS API     |
| Grant objectAdmin to Operator| Operator | storage.admin              | Token       | IAM Service |
| Create Key for Master SA     | Operator | iam.serviceAccountAdmin    | Token       | IAM API     |
| Create STS Agent             | Operator | serviceusage.service<br>.UsageConsumer (incl.)         | Token       | STS API     |
| Grant storage.admin to Worker| Operator | storage.admin              | Token       | IAM Service |
| Upload TSV File              | Operator | storage.objectAdmin        | Token       | GCS API     |
| Create/Run/Monitor Job       | Master   | storagetransfer.user (auto)| Credentials | STS API     |
| Perform Data Transfer        | Worker   | storage.admin              | Token       | GCS API     |
| List Transferred Files       | Operator | storage.objectViewer (auto)| ADC         | GCS API     |

**Légende:**
- Operator: GCP User. Master: User-managed Service Account. Worker: Google-managed STS Agent
- incl.: included in the role Project Owner of the Operator; auto : automatically granted

## 4.2 Monitoring & Exploitation

### Job de transfert sur le Cloud

Le job de transfert `job_transfer.py` est lancé automatiquement par `run_all_scripts.sh`. Pendant son exécution, il affiche dans la console une progression en pourcentage et en volume de données transférées (ex : "45.3% of 120.5 MB"). L'administrateur peut surveiller l'état détaillé du job via la console GCP, dans la section "Storage Transfer" -> "Jobs". Il y trouvera les métriques complètes : nombre de fichiers réussis, échecs éventuels, octets copiés, durée.

En cas d'échec, le script génère une erreur explicite dans les logs et stoppe l'exécution. Les causes fréquentes sont : URL source inaccessible (erreur 404), permissions STS insuffisantes, ou bucket de destination plein. L'administrateur peut consulter le fichier de log local (dans LOG_DIR/run_all_scripts_<timestamp>.log) pour obtenir le contexte exact de l'erreur. Pour lister les jobs de transfert actifs :

```bash
gcloud transfer jobs list
```

Pour surveiller un job spécifique depuis la ligne de commande :

```bash
gcloud transfer jobs describe <JOB_NAME> --project=$PROJECT_ID
```

### Job Spark sur le Cloud

Le job Spark est soumis via la commande gcloud dataproc jobs submit pyspark depuis `run_all_scripts.sh`. L'administrateur peut suivre son avancement de deux manières :

1. **Interface console** : via "Dataproc" -> "Jobs", il visualise les étapes (stages), les tâches (tasks), et les logs des executors.
2. **Ligne de commande** : pour lister les jobs soumis et récupérer les logs du driver en cas d'échec.

```bash
gcloud dataproc jobs list --cluster=main-cluster --region=us-central1 --state-filter=ACTIVE
gcloud dataproc jobs describe <JOB_ID> --region=us-central1
gcloud dataproc jobs wait <JOB_ID> --region=us-central1
```

Si le job échoue, l'administrateur peut consulter le driver log (dans la console Dataproc ou via la commande ci-dessus) pour identifier l'origine (ex : mémoire insuffisante, schéma incorrect, date invalide). Il peut ajuster les ressources allouées en modifiant les propriétés Spark via le paramètre --properties dans `run_all_scripts.sh`. Pour augmenter la mémoire des executors, ajouter par exemple :

```bash
--properties="spark.executor.memory=8g,spark.executor.cores=4"
```

### Script `run_all_scripts.sh`

Ce script est le point d'entrée unique pour l'administrateur. Il peut être lancé avec différents arguments pour exécuter partiellement le pipeline :

**Par défaut (sans option)** : exécute toutes les étapes (setup, transfert, Spark).

```bash
./run_all_scripts.sh
```

**Avec -f transfer** : ne lance que le transfert et le job Spark (saute le setup initial).

```bash
./run_all_scripts.sh -f transfer -s "2019-10-01 00:00:00" -e "2019-10-16 00:00:00"
```

**Avec -f spark** : ne lance que le job Spark (les données doivent déjà être présentes).

```bash
./run_all_scripts.sh -f spark -d "gs://$BUCKET_NAME/data/processed/run_manual"
```

Les paramètres -s (DATE_START), -e (DATE_END), et -d (DESTINATION) sont passés aux deux jobs. L'administrateur doit veiller à ce que DESTINATION soit bien un sous-chemin du bucket GCS (vérifié automatiquement par le script). Le script génère un identifiant unique basé sur l'horodatage pour chaque exécution, ce qui évite d'écraser les résultats précédents. Pour forcer un nom de job explicite :

```bash
./run_all_scripts.sh -f spark -d "gs://$BUCKET_NAME/data/processed/campaign_q3_2026"
```

### Fichiers log en local

Tous les logs sont redirigés à la fois vers la console (avec codes couleur pour une lecture aisée) et vers un fichier dans le répertoire LOG_DIR défini dans config.ini. Le nom du fichier suit le format run_all_scripts_<YYYYMMDD_HHMM>.log. Ces logs contiennent l'intégralité des sorties des sous-processus (transfert et Spark), avec les timestamps et les niveaux de log (INFO, ERROR). L'administrateur peut consulter ces fichiers pour :

Auditer les paramètres utilisés à chaque exécution.

```bash
grep -E "python3 | submit |   • [A-Z_]*: " ./log/run_all_scripts_*.log | tail -5
```

Diagnostiquer les erreurs sans avoir à relancer la console.

```bash
grep -i "error" ./log/run_all_scripts_$(date +"%Y%m%d_%H%M").log
```

Voir la configuration des workers Spark.

```bash
grep "Executor Configuration" ./log/run_all_scripts_*.log
```

Il est recommandé de purger régulièrement ce répertoire (ou de mettre en place une rotation des logs) pour éviter une saturation du stockage local. Si LOG_DIR est laissé vide dans config.ini, aucun fichier log n'est créé (seule la console est utilisée). Pour automatiser la rotation :

```bash
find ./log -name "run_all_scripts_*.log" -mtime +30 -delete
```

### 4.3 Gestion des Erreurs

## Tableau de résolution d'incidents courants

| Symptôme (Log / UI) | Cause Racine Probable | Action Corrective (Runbook) | Escalade |
|---------------------|----------------------|----------------------------|----------|
| `FAILED: Transfer job` + `PERMISSION_DENIED` | Compte de service STS sans rôle `storage.admin` sur bucket | `gcloud storage buckets add-iam-policy-binding ... --role=roles/storage.admin --member=serviceAccount:project-...@storage-transfer...` | Cloud Ops |
| `OutOfMemoryError` / `ExecutorLostFailure` | `MAX_NB_CACHED_ROWS` trop haut / partition trop grosse | 1. Baisser `MAX_NB_CACHED_ROWS` dans `config.ini`<br>2. Augmenter `--properties=spark.executor.memory=8g,spark.executor.cores=4` | Data Eng |
| `_SUCCESS` absent + `part-*` vides | Filtre dates exclut toutes les données / Schéma CSV changé | Vérifier `DATE_START/END` vs noms fichiers `YYYY-Mon.csv` + Valider header source | Data Scientist |
|`YarnScheduler: Lost executor ... bad node ... Killed ...`| mémoire insuffisante sur les workers | Même si la tâche est relancée avec succès, il vaut mieux augmenter la mémoire allouée aux workers | Cloud Ops |

# 5 Guide du Développeur

## 5.1 Lancement des scripts
Tous les scripts peuvent être exécutés individuellement. Alternativement; leur lancement en série a été automatisé. Il doivent tous être exécutés depuis la racine du projet.

### 5.1.1 Exécution simplifié
Le script d'orchestration **run_all_scripts.sh** lance intégralement ou partiellement les scripts du projet.

```bash
# Exécution complète du pipeline (setup, transfert, Spark)
./run_all_scripts.sh

# Exécution uniquement du transfert et du job Spark (sans setup initial)
./run_all_scripts.sh -f transfer -s "2019-12-01 00:00:00" -e "2019-12-31 00:00:00"

# Exécution uniquement du job Spark (transfert déjà effectué)
./run_all_scripts.sh -f spark -d "gs://mon-bucket/processed/experiment_v2"

# Lancement avec des paramètres personnalisés
./run_all_scripts.sh -f spark -s "2020-01-01 00:00:00" -e "2020-01-31 00:00:00" -d "gs://mon-bucket/data/processed/campaign_2026"
```

### 5.1.2 Lancement individualisé des jobs

#### `job_transfer.py`
```bash
python3 -m src.job_transfer --DATE_START="YYYY-MM-DD HH:MM:SS" --DATE_END="YYYY-MM-DD HH:MM:SS"
```

#### `job_spark.py`
```bash
BUCKET_NAME=`grep "^BUCKET_NAME =" ./src/config.ini | cut -d" " -f 3`
GCS_PATH="gs://$BUCKET_NAME"
FILEPATHS="src/config.ini src/job_spark.py src/lib_common.py"
for PATH_i in $FILEPATHS; do
    gcloud storage cp ./$PATH_i ${GCS_PATH}/$PATH_i
done

gcloud dataproc jobs submit pyspark $GCS_PATH/src/job_spark.py \
--cluster=main-cluster --region=us-central1 \
--files="$GCS_PATH/src/config.ini" \
--py-files="$GCS_PATH/src/lib_common.py" -- \
--DATE_START="YYYY-MM-DD HH:MM:SS" --DATE_END="YYYY-MM-DD HH:MM:SS" \
--DESTINATION="${GCS_PATH}/data/processed/run_20260714"
```
---

## 5.2 Description du code

### 5.2.1 `lib_common.py`
Ce fichier centralise les utilitaires partagés entre `job_transfer.py` et `job_spark.py`. Il définit la dataclass **CONF_VARS** contenant les paramètres de configuration et expose notamment :

- `apply_config_values()` : chargée la configuration depuis `config.ini` et initialise le logging.
- `clean_data()` : supprime les sessions avec plusieurs `user_id` via une jointure *left_anti*.
- `transform()` : orchestre les agrégations et transformations de features.

### 5.2.2 `job_transfer.py`
Transfère les fichiers bruts AWS → GCS via Storage Transfer Service (STS) :

1. Génère la liste des fichiers à traiter avec `list_files_to_process()`.
2. Crée un fichier **TSV** contenant les URLs sources.
3. Upload du TSV dans le bucket et lance le job STS.
4. Montre la progression du transfert (ex: `45.3% of 120.5 GB`) jusqu’à succès ou échec.

### 5.2.3 `job_spark.py`
Exécute le pipeline ETL sur le cluster Dataproc :

1. Initialise Spark (`create_spark_session`) avec le connecteur GCS.
2. Extrait les données brutes en DataFrame.
3. Crée les features.
4. Écrit le résultat final en CSV sur GCS.
---
**Calcul des Features**
| Table | Feature (Type / Intermédiaire) | Formule | Description |
| :--- | :--- | :--- | :--- |
| **Produit** | `purchased` (`int`) | `max(event_type = "purchase")` groupé par produit et session | Vaut 1 si un achat a eu lieu pour ce produit lors de la session, 0 sinon. |
| **Produit** | `num_views_product` (`int`) | `sum(event_type = "view")` groupé par produit et session | Compte le nombre de fois que ce produit a été vu dans la session. |
| **Session** | `num_views_session` (`int`) | `countDistinct(product_id)` où `event_type = "view"` groupé par session et utilisateur | Compte le nombre de produits distincts vus dans cette session. |
| **Session** | `start_time` (`string`) | `format(temp_start_dt, "HH:mm")` | Extrait l'heure de début de la session à partir de l'horodatage `temp_start_dt`. |
| **Session** | `start_weekday` (`string`) | `format(temp_start_dt, "EEEE")` | Extrait le nom du jour de la semaine de l'horodatage `temp_start_dt`. |
| **Session** | `duration` (`long`) | `unix_timestamp(temp_end_dt) - unix_timestamp(temp_start_dt)` | Calcule la durée de la session en secondes. |
| **Session** | `num_prev_sessions` (`int`) | `count(user_session)` sur une fenêtre de sessions précédentes par utilisateur | Compte le nombre de sessions de ce même utilisateur ayant commencé avant la session actuelle. |
| **Session & Produit** | `num_prev_product_views` (`int`) | `sum(num_views_product)` sur une fenêtre de sessions précédentes, par produit et utilisateur | Additionne les vues de ce produit lors des sessions antérieures de l'utilisateur. |
---

### 5.2.4 `config.ini`
```ini
# STORAGE: S3/GCS sources and raw data paths
# SPARK: date range and processing thresholds
# LOGGING: console/file output settings
# GENERAL: helper flags and format references
# TESTING: directory & files involved in testing of Spark job

[STORAGE]
# Mandatory provision of these 3 values
SOURCE_URL_DIR = https://blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/9c15cb
BUCKET_NAME = blent_spark_bucket_0
REGION = us-central1

# RAW DIR is Transfer Job's output and becomes Spark Job's input
REL_RAW_DIR = data/raw

# Char "%" is doubled for Python's compatibility
DATE_FORMAT = %%Y-%%m-%%d %%H:%%M:%%S

[SPARK]
# Default arguments' values for testing Spark job
DATE_START = 2019-12-16 00:00:00
DATE_END = 2020-01-15 00:00:00
REL_PROCESSED_DIR = data/processed

# Empirical threshold : nb of rows of a DataFrame that executors can handle
# Used in transformation optimization by caching a sdf involved in 2 branches
# 1 source raw file for 1 month of data represents around 60 million rows
MAX_NB_CACHED_ROWS = 200000000

[LOGGING]
LOG_LEVEL = INFO
LOG_FORMAT = ▶️ %%(asctime)s: %%(message)s

# Leave log directory empty to avoid creating a log file
LOG_DIR = ./log

[GENERAL]
HELP_DATE_FORMAT = YYYY-MM-DD HH:MM:SS
DEBUG_ENABLED = False
# DEBUG_ENABLED = True

[TESTING]
TEST_REL_DIR = ./tests
MOCK_INPUT_FILE = mock_data.csv
EXPECTED_OUTPUT = expected_output.csv
```
   
## 5.3 Tests
Les tests se font en local et sont lancés depuis la racine. LEur but est de tester :
- l'accès en lecture/écriture au bucket GCS
- l'extraction et la transformation des données dans `job_spark`

```bash
# Run all tests
    pytest -v tests/

# Run all tests and log to a file
    pytest tests/test_job_spark.py -vv --disable-warnings 2>&1 |
    tee ./log/test_job_spark_`date +"%Y%m%d_%H%M"`.log

# Test a single feature
    pytest tests/test_job_spark.py -k <feature_name> -vv --disable-warnings

```

## 5.4 Architecture et bonnes pratiques
- **Caching Spark** : activé via `MAX_NB_CACHED_ROWS` dans `config.ini`.
- **Optimisation des partitions** : calcul dynamique avec `get_optimal_nb_partitions()`.
- **Monitoring** : 
  ```bash
  # STS
  gcloud transfer jobs list --project=$PROJECT_ID

  # Dataproc
  gcloud dataproc jobs describe <JOB_ID> --region=us-central1
  ```
- **Journalisation** : logs colorés en console, fichiers horodatés dans `LOG_DIR/run_all_scripts_*.log`.
- **Nettoyage des utilisateurs** : suppression des sessions liées à plusieurs `user_id` via une jointure *left_anti*.
- **Filtrage temporel** : uniquement les événements compris entre `DATE_START` et `DATE_END`.
- **Preuves de concept** : 2 notebooks Jupyter avec exécution locales de Spark sur un échantillon de données.
  - [nbook_prototype.ipynb](../src/nbook_prototype.ipynb) sur stockage local 
  - [nbook_cloud.ipynb](../src/nbook_cloud.ipynb) sur GCS.

---  

# 6. Guide du Data Scientist

## 6.1 Contexte

En tant que Data Scientist, vous disposez d'un pipeline ETL automatisé qui extrait les données comportementales des utilisateurs du site e-commerce, les transforme en caractéristiques pertinentes, et produit une base d'apprentissage prête à être utilisée pour vos modèles prédictifs. Ce pipeline est conçu pour être exécuté à la demande, sur n'importe quelle plage de dates, sans intervention technique de votre part.

Le job Spark produit une table de sortie à la maille **(session utilisateur × article)**. Chaque ligne contient :

*   Informations sur l'article : `prix`, `marque`, `catégorie`.
*   Indicateurs comportementaux : nombre de vues, achat ou non (`purchased`).
*   Caractéristiques de session : durée, jour de début, heure.
*   Historique utilisateur : sessions précédentes, vues antérieures du produit.

Cette table est directement utilisable pour entraîner un modèle de classification binaire (achat finalisé ou non) ou pour des analyses exploratoires. Vous n'avez pas besoin de connaître les détails techniques de l'infrastructure Cloud (Dataproc, GCS, STS). Tout est orchestré par un script unique qui vous demande uniquement de spécifier la période d'extraction et l'emplacement de sauvegarde des résultats.

<img src="../docs/infographic_workflow_details.png" width="90%">

## 6.2 Instructions d'utilisation

### 6.2.1 Prérequis

Avant d'exécuter le pipeline, assurez-vous de disposer des éléments suivants :

1.  Un accès au projet GCP et au bucket GCS (vos identifiants doivent avoir le rôle `storage.objectAdmin`).
2.  Les outils `gcloud CLI` et `Python 3.9+` installés sur votre machine.
3.  Le dépôt Git du projet cloné localement.
4.  Le script `run_all_scripts.sh` active l'environnement virtuel automatiquement mais pour lancer individuellement le job Spark ou le script de transfert, il doit être activé par cette commande:
    ```bash
    source venv_spark/bin/activate
    ```

### 6.2.2 Utilisation du script `run_all_scripts.sh`

Le script d'orchestration `run_all_scripts.sh` est le point d'entrée recommandé au moins pour la première exécution du pipeline car il couvre :
- l'installation de l'environnement
- la gestion de l'authentification
- le transfert des fichiers bruts depuis des URL vers GCS 
- la soumission du job Spark sur Dataproc.

#### Syntaxe générale

```bash
./run_all_scripts.sh -f <phase> -s "YYYY-MM-DD HH:MM:SS" -e "YYYY-MM-DD HH:MM:SS" -d "gs://<BUCKET_NAME>/<chemin_sortie>"
```

#### Paramètres

| Paramètre | Requis | Description |
| :--- | :--- | :--- |
| `-f` | Non | Phase de démarrage. Valeurs : `1` ou `all` (default : setup env & data services + transfert + Spark), `2` ou `auth` (omet setup env), `3` ou `transfer` (transfert + Spark), `4` ou `spark` (Spark uniquement). |
| `-s` | **Oui** | `DATE_START` au format `YYYY-MM-DD HH:MM:SS` (ex: `"2019-12-01 00:00:00"`). |
| `-e` | **Oui** | `DATE_END` au format `YYYY-MM-DD HH:MM:SS` (ex: `"2020-01-15 00:00:00"`). Doit être > `DATE_START`. |
| `-d` | Non | `DESTINATION` : chemin complet GCS où sera enregistré le CSV final. Ex: `"gs://bucket9/data/processed/campaign_q4_2019"`. Si omis, utilise le chemin par défaut de `config.ini`. |

#### Exemples concrets

```bash
# Extraction sur deux mois (décembre 2019 et janvier 2020), avec transfert automatique
./run_all_scripts.sh -f all -s "2019-12-01 00:00:00" -e "2020-01-31 00:00:00" -d "gs://bucket9/data/processed/experiment_dec_jan"

# Extraction sur une quinzaine (les données sont déjà dans GCS, on saute le transfert)
./run_all_scripts.sh -f spark -s "2019-10-01 00:00:00" -e "2019-10-15 00:00:00" -d "gs://bucket9/data/processed/test_quick"

# Extraction sur une période sans spécifier de destination (utilise le chemin par défaut)
./run_all_scripts.sh -f spark -s "2020-02-01 00:00:00" -e "2020-02-28 00:00:00"
```

#### Que se passe-t-il pendant l'exécution ?

1.  **Phase de transfert (si activée)** : Le script vérifie si les fichiers CSV mensuels correspondant à votre plage de dates sont déjà présents dans le bucket GCS. Si ce n'est pas le cas, il les copie depuis les URLs publiques via Storage Transfer Service. Vous verrez dans la console une progression en pourcentage et en volume (ex : `Transfer in progress: 45.3% of 120.5 MB ⏳`). Cette phase peut durer de quelques minutes à plusieurs dizaines de minutes selon le volume.
2.  **Phase Spark** : Le script soumet le job Spark au cluster Dataproc. Vous verrez l'URL du job dans la console (ex : `Job [spark-etl-20260714_1530] submitted`). Pendant l'exécution, la console affiche les étapes clés : extraction des données, nettoyage, calcul des caractéristiques, écriture. Chaque étape est accompagnée du nombre de lignes traitées (ex : `🧹 15_234_567 rows after cleaning`).

### Utilisation directe du job Spark (sans orchestration)
Si vous souhaitez exécuter le job Spark indépendamment (par exemple pour des tests itératifs ou pour éviter de relancer le transfert), vous pouvez utiliser directement `job_spark.py` :

```bash
python3 -m src.job_spark --DATE_START="2019-11-01 00:00:00" --DATE_END="2019-11-30 00:00:00" --DESTINATION="gs://mon-bucket/data/processed/test_local"
```

Les arguments sont les mêmes que pour `run_all_scripts.sh`, mais sans l'option `-f`. Ce mode est utile pour le débogage ou pour des exécutions rapides sur des volumes réduits (pensez à activer `DEBUG_ENABLED=True` dans `config.ini` pour limiter à 1 million de lignes).

## 6.3 Localisation des résultats

### 6.3.1 Chemin de sortie

Le fichier CSV final est écrit dans le répertoire spécifié par le paramètre `DESTINATION`. Si vous avez utilisé `run_all_scripts.sh`, le script ajoute automatiquement un sous-répertoire horodaté (ex : `run_20260714_1530`) pour éviter d'écraser les résultats précédents. Le chemin complet sera donc du type :

```text
gs://<BUCKET_NAME>/data/processed/campaign_q4_2019/run_20260714_1530/
```

### 6.3.2 Contenu du répertoire

*   Plusieurs fichiers `part-*.csv` (le nombre dépend du volume de données et du partitionnement). Chaque fichier contient une partie des lignes de la table de sortie.
*   Un fichier `_SUCCESS` (vide) qui atteste que l'écriture s'est terminée sans erreur. **Sa présence est le signal que vous pouvez récupérer les données.**

### 6.3.3 Récupération des données

Pour télécharger les résultats sur votre machine locale, utilisez la commande `gcloud storage` :

```bash
# Télécharger tous les fichiers CSV (récursif)
gcloud storage cp -r gs://<BUCKET_NAME>/data/processed/campaign_q4_2019/run_20260714_1530/ ./mon_dossier_local/

# Ou bien concaténer les fichiers en un seul CSV
gcloud storage cat gs://<BUCKET_NAME>/data/processed/campaign_q4_2019/run_20260714_1530/part-*.csv > ma_table_finale.csv
```

### 6.3.4 Vérification des données

Après téléchargement, vous pouvez charger le CSV dans un DataFrame Pandas pour une inspection rapide :

```python
import pandas as pd

df = pd.read_csv("ma_table_finale.csv")
print(df.shape)
print(df.head())
print(df[["purchased", "num_views_product", "duration", "num_prev_sessions"]].describe())
```

### 6.3.5 Format de la table de sortie

Les colonnes sont les suivantes (dans l'ordre) :

<div style="overflow-x: auto">
  <table border="1" style="border-collapse: collapse; font-size: 11px;">
    <tbody>
      <tr>
        <td><strong>Colonne</td>
        <td>product<br>id</td>
        <td>category<br>code</td>
        <td>brand</td>
        <td>price</td>
        <td>user<br>session</td>
        <td>purchased</td>
        <td>num<br>views<br>product</td>
        <td>num<br>views<br>session</td>
        <td>start<br>time</td>
        <td>start<br>weekday</td>
        <td>duration</td>
        <td>num<br>prev<br>sessions</td>
        <td>num<br>prev<br>product<br>views</td>
      </tr>
      <tr>
        <td><strong>Type</strong></td>
        <td>string</td>
        <td>string</td>
        <td>string</td>
        <td>double</td>
        <td>string</td>
        <td>int<br>(0/1)</td>
        <td>int</td>
        <td>int</td>
        <td>string<br>(HH:MM)</td>
        <td>string<br>(ex: "Monday")</td>
        <td>long<br>(secondes)</td>
        <td>int</td>
        <td>int</td>
      </tr>
    </tbody>
  </table>
</div>


## 6.4 Bonnes pratiques

1.  **Conservez toujours le fichier `_SUCCESS`** comme preuve d'intégrité.
2.  Pour vos expériences, utilisez des noms de destination explicites (ex : `experiment_model_v2_seed42`) et datez-les pour garder un historique.
3.  Si vous travaillez sur des volumes très importants (> 3 mois), prévoyez suffisamment de temps (le job Spark peut prendre 30 à 60 minutes) et vérifiez que le cluster Dataproc a des ressources suffisantes (au besoin, contactez l'administrateur pour augmenter la mémoire des executors).
4.  En cas d'échec, consultez le fichier log local `./log/run_all_scripts_<timestamp>.log` ou l'interface Dataproc pour identifier l'erreur. Les causes fréquentes sont :
    *   Date mal formatée (respectez l'espace entre date et heure : `YYYY-MM-DD HH:MM:SS`).
    *   Plage de dates trop longue (> 6 mois).
    *   Bucket de destination plein.


# 7. Annexe: Glossaire et Acronymes
Indispensable pour les nouveaux arrivants et la validation métier.

| Terme / Acronyme | Définition |
| :--- | :--- |
| **ADC** | **Application Default Credentials** : Méthode d'authentification utilisée par les APIs Google pour récupérer automatiquement les identifiants de l'utilisateur connecté. |
| **CLI** | **Command Line Interface** : Interface de ligne de commande (notamment la `gcloud CLI`) utilisée pour piloter les services cloud. |
| **Dataproc** | Service géré de Google Cloud permettant d'exécuter des clusters **Spark** et **Hadoop** de manière distribuée. |
| **GCP** | **Google Cloud Platform** : L'écosystème cloud hébergeant l'ensemble de l'infrastructure et des services du projet. |
| **GCS** | **Google Cloud Storage** : Service de stockage d'objets utilisé pour conserver les données brutes, les scripts et les résultats finaux. |
| **HDFS** | **Hadoop Distributed File System** : Système de fichiers distribué utilisé par Dataproc comme alternative de stockage. |
| **IAM** | **Identity and Access Management** : Système de gestion des permissions permettant de définir les rôles (ex: *Storage Admin*) sur les ressources GCP. |
| **Left Anti Join** | Type de jointure Spark utilisée pour supprimer les sessions incohérentes associées à plusieurs identifiants utilisateurs. |
| **Maille** | Niveau de granularité de la donnée ; la table de sortie est à la maille **session utilisateur × produit**. |
| **OOM** | **Out Of Memory** : Erreur de saturation mémoire pouvant survenir sur le cluster si le volume de données dépasse les ressources des exécuteurs. |
| **S3** | **Simple Storage Service** : Service de stockage d'Amazon (AWS) où sont initialement hébergés les fichiers CSV sources. |
| **STS** | **Storage Transfer Service** : Service GCP utilisé pour automatiser le transfert des fichiers depuis AWS S3 vers les buckets GCS. |
| **TSV** | **Tab-Separated Values** : Format de fichier intermédiaire généré pour lister les URLs sources lors de l'utilisation du service STS. |
| **UTC** | **Coordinated Universal Time** : Standard temporel universel utilisé pour le formatage des horodatages et des paramètres de dates. |
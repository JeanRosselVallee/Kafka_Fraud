<div align="center">
  <a href="https://blent.ai">
    <img src="https://blent-static-media.s3.eu-west-3.amazonaws.com/images/logo/logo_blent_300x.png" alt="Logo Blent.ai" width="200" />
  </a>

  <h2 align="center">Détection des anomalies de flux financiers avec Apache Kafka</h2>

  <p align="center">
    Projet Data Engineering - <a href="https://blent.ai">Blent.ai</a>
    <br />
    <a href="https://blent.ai/app/projects" target="_blank"><strong>Explorer tous les projets »</strong></a>
</div>

<div align="center"><img src="https://blent-static-media.s3.eu-west-3.amazonaws.com/images/projects/badge_kafka_fraud.svg" width="120" alt="Badge du projet" /></div>

## À propos du projet

Une grande banque en ligne souhaite améliorer sa réactivité face aux transactions frauduleuses afin de servir au mieux ses clients. Pour cela, elle souhaite donc **détecter en temps réel les transactions frauduleuses** qui pourraient survenir sur les comptes de ses clients.

Dans son système, l'entreprise enregistre toutes les transactions dans une base de données. Après réflexions au sein de l'équipe technique, l'entreprise a décidé **d'utiliser Apache Kafka pour y stocker toutes les transactions financières en temps réel** de ses clients. Elle souhaite donc pouvoir utiliser ce système afin d'effectuer des analyses en temps réel et faire remonter les potentielles transactions frauduleuses.

En tant que Data Engineer, il vous est demandé de **mettre en place un consumer qui puisse analyser toutes les transactions en temps réel** et faire remonter les transactions potentiellement frauduleuses. Afin d'aider les Data Scientists à calibrer un modèle de Machine Learning qui puisse identifier les transactions frauduleuses, l'entreprise utilise des robots qui ajoutent artificiellement des fraudes dans les données.

> TODO : Compléter cette partie pour apporter plus d'informations sur le contexte du projet.

## Étapes du projet

- [ ] Construire le consumer pour analyser les transactions
- [ ] Calibrer un modèle de régression logistique
- [ ] Détecter en temps réel les cas de fraude
- [ ] Déployer le consumer dans le Cloud
- [ ] Publier le code source et les résultats sur GitHub

La description des étapes est disponible sur la page associée au projet.

> TODO : Cocher les cases au fur et à mesure de l'avancement.

## Structure du projet

Le dépôt Git contient les éléments suivantes.

- `src/` contient les codes sources Python principaux du projet, en particulier les codes du producer (déjà présent) et du consumer.
- `data/` contient les données du projet.
- `config/` contient les configurations et paramètres du projet.
- `LICENSE.txt` : licence du projet.
- `requirements.txt` : liste des dépendances Python nécessaires.
- `README.md` : fichier d'accueil.

## Premiers pas

Les instructions suivantes permettent d'exécuter le projet sur son PC.

### Pré-requis

Le projet nécessite Python 3 d'installé sur le système.

> TODO : Ne pas hésiter à compléter/adapter cette partie en fonction des dépendances logicielles.

### Installation

1. Cloner le projet Git.
	```
	git clone https://github.com/blent-ai/Projet-Data-Engineering-Kafka-Fraud.git
	```
2. Installer les dépendances du fichier `requirements.txt` dans un environnement virtuel.

	**Linux / MacOS**
	```
	python3 -m venv venv/
	source venv/bin/activate
	pip install -r requirements.txt
	```
	**Windows**
	```
	python3 -m venv venv/
	C:\<chemin_dossir>\venv\Scripts\activate.bat
	pip install -r requirements.txt
	```

> TODO :
> - Remplir le fichier `requirements.txt` pour y ajouter les dépendances (Pandas, PySpark, FindSpark, etc).
> - Compléter la procédure d'installation pour l'adapter en fonction des besoins (cluster Dataproc, EMR, etc).

### Démarrage

> TODO : Expliquer en quelques lignes et avec des exemples de ligne de commande comment l'utilisateur peut entraîner ou utiliser lui-même le modèle. 

## Licence

*Ce projet est proposé par <a href="https://blent.ai">Blent.ai</a>. Les données utilisées pour ce projet peuvent être soumises à des droits d'auteur et de propriété intellectuelle. Blent.ai ne peut être responsable des utilisations faites des données utilisées dans le cadre de ce projet.*

> TODO : Ajouter les licences supplémentaires au projet (autres données, outils propriétaires, etc).

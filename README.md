# FEMA NFIP — Plateforme Big Data d'intégration et d'unification des données d'assurance

**AbAssurance × AssurePlus | CDC · Apache Kafka · Apache Spark · Apache Iceberg · Data Engineering**

> Concevoir une plateforme d'intégration continue de deux systèmes d'assurance hétérogènes, sans interrompre leurs applications transactionnelles, puis exposer un modèle analytique commun, traçable et testable.

Ce dépôt contient le code et les expérimentations d'une plateforme de données fondée sur les données publiques **FEMA National Flood Insurance Program (NFIP)**, enrichies et adaptées pour représenter deux assureurs fictifs. Les données FEMA constituent le matériau du démonstrateur ; **AbAssurance** et **AssurePlus** sont des entités de simulation, pas des systèmes de production de la FEMA.

Le projet est développé dans le cadre d'un parcours de spécialisation Big Data. Il vise aussi à construire un socle de Data Engineering réutilisable : ingestion, capture des changements, modèles canoniques, contrôle d'intégrité, exposition analytique et infrastructure reproductible.

## Sommaire

1. [Contexte et objectifs](#1-contexte-et-objectifs)
2. [Architecture](#2-architecture)
3. [Périmètre et statut](#3-périmètre-et-statut)
4. [Modèle de données et mapping](#4-modèle-de-données-et-mapping)
5. [Pipeline Bronze → Silver → Gold](#5-pipeline-bronze--silver--gold)
6. [Environnements Spark](#6-environnements-spark)
7. [Structure du dépôt](#7-structure-du-dépôt)
8. [Tests et CI/CD](#8-tests-et-cicd)
9. [Exploitation locale et restitution BI](#9-exploitation-locale-et-restitution-bi)
10. [Infrastructure cible et prochaines étapes](#10-infrastructure-cible-et-prochaines-étapes)
11. [Gouvernance et sécurité](#11-gouvernance-et-sécurité)
12. [Références et périmètre documentaire](#12-références-et-périmètre-documentaire)

## 1. Contexte et objectifs

Deux organismes fictifs spécialisés dans l'assurance contre les inondations doivent être intégrés tout en conservant leurs systèmes transactionnels :

| Organisme | SGBD du démonstrateur | Fonction |
|---|---|---|
| **AbAssurance** | Microsoft SQL Server | Contrats, assurés, unités de police et caractéristiques métier |
| **AssurePlus** | Oracle Database | Contrats, titulaires, assurés, sinistres et caractéristiques métier |

Les schémas sources ne sont **pas identiques**. Il ne s'agit ni de copier aveuglément les tables d'un SGBD vers l'autre, ni de considérer que leurs identifiants ont une signification globale.

Les besoins fonctionnels majeurs sont :
- synchroniser les changements sans interrompre les applications sources ;
- préserver la provenance, les événements bruts et les métadonnées CDC ;
- harmoniser les polices, titulaires, sinistres et caractéristiques dans **Silver** ;
- assurer l'intégrité référentielle et éviter les duplications causées par les jointures ;
- produire des statistiques clients, contrats, sinistres, financières et une matrice des doublons ;
- préparer une exposition SQL/BI et un déploiement d'infrastructure reproductible.

**Principe d'architecture :** le choix des composants est motivé par leurs garanties et leurs compromis opérationnels. Talend/Hadoop sont étudiés comme alternatives dans le dossier de conception ; ils ne constituent pas la pile effectivement retenue dans ce démonstrateur.

## 2. Architecture

```text
  FEMA NFIP (open data)
          │
          ▼
  Spark 4.2 — préparation, adaptation et modélisation des données sources
          │
          ├───────────────────────────┐
          ▼                           ▼
  SQL Server                      Oracle Database
  AbAssurance                     AssurePlus
          │                           │
          └───────────┬───────────────┘
                      ▼
           Debezium / Kafka Connect
                      │
                      ▼
                Apache Kafka
                      │  Debezium CDC, topic / partition / offset
                      ▼
       Spark 4.1 Structured Streaming
                      │
                      ▼
            Apache Iceberg — Bronze
          événements et provenance bruts
                      │
                      ▼
            Apache Iceberg — Silver
       politiques, sinistres, titulaires,
       extensions et identité canonique
                      │
                      ▼
           Gold / données analytiques
                      │
                      ▼
          Trino / Power BI / restitution
```

La **cible d'industrialisation** ajoute un stockage objet compatible S3 (Ceph RGW), un catalogue Iceberg partagé, Kubernetes et ses opérateurs, Terraform pour l'infrastructure, Airflow pour l'orchestration et Prometheus/Grafana pour l'observabilité. **La présence de ces composants dans l'architecture cible ne signifie pas qu'ils sont tous déployés aujourd'hui.**

Le stockage Iceberg local vérifié dans les scripts utilise le catalogue Hadoop `lakehouse` et le warehouse `/lakehouse/warehouse` **à l'intérieur des conteneurs Spark**. Cette configuration locale n'est pas une architecture de catalogue partagé prête à être exposée directement à Trino dans le cloud.

## 3. Périmètre et statut

| Domaine | État documenté |
|---|---|
| Préparation FEMA, adaptation synthétique et bases sources | Scripts de modélisation, DDL, mappings et chargement présents |
| Debezium → Kafka → Bronze Iceberg | Code et tests d'intégration Kafka/Iceberg présents |
| Canonicalisation Policy / Claim | Implémentation Spark Silver présente |
| Extensions métier Silver | Implémentation de huit extensions, contrôles d'intégrité présents |
| Rapprochement d'identité des titulaires | Implémentation `holder_identity.py` et `policy_holder_link.py` présente |
| Restitution Power BI | Export Parquet du portefeuille réalisé localement |
| Gold complet, cinq restitutions finales | Développement / validation de bout en bout à finaliser |
| Trino, Ceph, Kubernetes, Terraform/GCP | Architecture cible ; déploiement non attesté par ce dépôt |
| SonarQube Cloud | Job GitHub Actions configuré, exécution conditionnelle |

**Mesures observées sur le démonstrateur local le 20 novembre 2026 :**
- **117 437 polices** exportées pour Power BI ;
- huit extensions Silver jointes sans polis manquantes lors de cet export ;
- **36 contrôles d'intégrité Silver réussis** lors de l'exécution locale du script de validation.

Ces résultats correspondent à un instantané et à un périmètre précis ; ils ne doivent pas être extrapolés à une mise en production, à un taux de couverture global ou à des garanties de disponibilité.

## 4. Modèle de données et mapping

### 4.1 Deux modèles sources distincts

**SQL Server — AbAssurance :** le modèle relationnel est organisé notamment autour de `Holder`, `Policy`, `PolicyUnit`, `PrivateClient` et des extensions liées à l'unité ou au contrat : `PolicyTerms`, `Building`, `Risks`, `Property`, `PropertyUsage`, `PolicyRating`, `PremiumFinancial` et `HolderClassification`.

**Oracle — AssurePlus :** les entités source sont identifiées par des noms propres à l'environnement Oracle (notamment `POLICY`, `HOLDER`, `PRIVATE_CLIENT` et les extensions `POLICY_*`). Leur grain et leur encodage de clés ne doivent pas être supposés identiques à ceux de SQL Server.

Les mappings réels sont implémentés ici :
- `data/source_system/bootstrap/column_mapping.py` ;
- `data/source_system/dbs/oracle/migrations/oracle_mappings.py` ;
- `data/source_system/dbs/sql_server/migrations/sql_server_init.sql` ;
- `data/source_system/dbs/oracle/migrations/oracle_init.sql`.

### 4.2 Identifiants canoniques

La clé d'une police est définie par la provenance et l'identifiant source :

```text
canonical_policy_key = source_system + ":" + policy_record_id

SQLSERVER:3039 ≠ ORACLE:3039
```

Le code de canonicalisation se trouve dans `data_platform/spark/canonical_policy_claim.py`. La provenance des événements (topic, partition, offset et métadonnées) reste associée aux données Silver.

**Ne pas confondre :**
- `PolicyId` et `PolicyUnitId` côté SQL Server ;
- `POLICY_RECORD_ID` côté Oracle ;
- `canonical_policy_key` dans le modèle analytique ;
- `canonical_holder_id` pour les titulaires rapprochés.

Les polices des deux assureurs représentent **des enregistrements métier distincts**. Le modèle n'effectue pas de déduplication transversale arbitraire de contrats.

### 4.3 Titulaires et jointures

Les politiques sont liées aux titulaires au moyen des relations **déjà modélisées** :

```text
SQL Server : Policy.HolderId       → Holder.HolderId
Oracle     : POLICY.HOLDER_CONTEXT → HOLDER.HOLDER_CONTEXT
                         │
                         ▼
                canonical_holder_id
```

Les scripts :
- `data_platform/spark/holder_identity.py` produit des correspondances de titulaires, notamment à partir des emails normalisés quand ils sont disponibles ;
- `data_platform/spark/policy_holder_link.py` lie les polices aux titulaires canoniques et vérifie que le **grain des polices n'est pas modifié**.

Un même titulaire peut détenir plusieurs polices. Les extensions 1:1 ne doivent jamais multiplier le nombre de polices lors d'un `JOIN`. Un sinistre est identifié par sa propre clé canonique et référence une police ; toute hypothèse plus restrictive sur la cardinalité des sinistres doit être testée explicitement.

## 5. Pipeline Bronze → Silver → Gold

### Bronze : conservation des événements CDC

`data_platform/spark/bronze_ingestion.py` consomme les messages Debezium depuis Kafka vers `lakehouse.bronze.cdc_events`. Le Bronze conserve notamment :

- `source_system`, `topic`, `partition`, `offset` ;
- `kafka_timestamp`, `key_raw`, `value_raw`, `ingested_at`.

Les couples **(topic, partition, offset)** identifient la position d'un événement dans Kafka. Les offsets ne définissent **pas** d'ordre global entre plusieurs partitions ou topics. Le choix du checkpoint et de `startingOffsets` conditionne les garanties de reprise ; un nouveau checkpoint initialisé à `latest` ne remplace pas un protocole de bascule validé.

### Silver : état canonique

Principaux modules :

| Module | Fonction |
|---|---|
| `canonical_policy_claim.py` | Normalisation et reconstruction de l'état Policy/Claim à partir du CDC |
| `canonical_silver_extensions.py` | Reconstruction des huit extensions métier Silver |
| `schema_inventory.py` | Inventaire / contrat des schémas sources |
| `holder_identity.py` | Rapprochement des titulaires |
| `policy_holder_link.py` | Jointure canonique titulaire–police sans modification du grain |
| `publish_policy_view.py` | Publication de la vue enrichie du portefeuille |

La reconstruction actuelle de certaines tables Silver est **batch / create-or-replace** ; elle ne doit pas être présentée comme un `MERGE` transactionnel incrémental par ligne tant que cette évolution n'a pas été implémentée et validée.

### Gold et restitution

La couche Gold doit publier cinq familles d'indicateurs métier :
1. répartition des clients ;
2. statistiques des contrats ;
3. statistiques des sinistres ;
4. statistiques financières ;
5. matrice des doublons.

Le portefeuille de polices est exporté par `data_platform/spark/export_powerbi.py` en Parquet pour Power BI. L'export impose l'unicité des clés attendues et vérifie que les jointures avec les extensions ne modifient pas le nombre de polices.

La restitution cible est **un seul rapport Power BI** contenant ces cinq domaines. Les exports locaux et le rapport BI constituent une étape de démonstration ; la connexion SQL à Trino et la publication Gold dans une infrastructure partagée restent à industrialiser.

## 6. Environnements Spark

**Deux versions de Spark sont utilisées volontairement.** Elles ne sont ni interchangeables ni des erreurs de configuration.

| Runtime | Usage | Exécution |
|---|---|---|
| **Spark 4.2** | Préparation FEMA, modélisation, enrichissement, création/alimentation des bases sources | WSL / environnement de développement |
| **Spark 4.1** | Intégration CDC Kafka, ingestion Iceberg Bronze, Silver, contrôles et export analytique | Image Docker Spark avec Iceberg |

Les scripts d'intégration utilisent notamment :

```text
org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0
org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0
```

La compatibilité binaire dépend de Spark et de Scala. **Ne pas lancer les scripts Iceberg du runtime d'intégration directement avec `spark-submit` de Spark 4.2 sans la dépendance correspondante.**

## 7. Structure du dépôt

```text
.
├── .github/workflows/
│   └── fema-quality.yml
├── configs/
│   ├── schema.py
│   └── pipelines/
├── crds/
│   └── datapipeline-crd.yaml
├── data/
│   ├── modeling/                  # modélisation FEMA
│   ├── ingestion/                 # sources et enrichissement
│   ├── source_system/
│   │   ├── bootstrap/            # mappings de colonnes
│   │   ├── dbs/                  # DDL et migrations Oracle/SQL Server
│   │   └── seed/                 # chargement initial
│   └── spark_utils/
├── data_platform/
│   ├── spark/                     # Bronze, Silver, identité et export BI
│   ├── tests/
│   │   ├── unit/
│   │   └── integration/
│   ├── runtime/
│   ├── config/
│   └── compiler/
├── infrastructure/
│   ├── debezium/
│   ├── docker/
│   └── testing/
└── docs/
    ├── decisions.md
    └── testing/
```

Le CRD `DataPipeline` et les fichiers sous `configs/pipelines/` décrivent **une cible de contrôle et de déploiement**. Certains exemples de configuration peuvent refléter un état antérieur du mapping des assureurs ; se référer aux DDL et scripts effectivement exécutés pour documenter le démonstrateur actuel.

Les datasets volumineux, les checkpoints et les volumes de stockage ne sont pas distribués avec le code source du dépôt.

## 8. Tests et CI/CD

Le projet privilégie des **invariants vérifiables**, plutôt que la simple affirmation qu'un service est « up » :

- unicité et non-nullité des clés ;
- absence de références orphelines ;
- conservation du grain lors des jointures ;
- déterminisme de la réduction CDC dans les conditions testées ;
- cohérence des extensions ;
- résultats attendus après mise à jour d'un enregistrement source.

### Exécution locale

Depuis la racine du dépôt, sous WSL avec Docker disponible :

```bash
# Phase 1 : tests unitaires et smoke test Spark/Iceberg isolé
bash infrastructure/testing/run_tests.sh

# Phase 2 : Kafka -> Bronze -> Silver, environnement d'intégration isolé
bash infrastructure/testing/run_kafka_e2e.sh

# Contrôles en lecture seule sur le warehouse Iceberg local existant
bash infrastructure/testing/run_silver_readonly.sh
```

**Attention :** `run_silver_readonly.sh` lit le **warehouse persistant réel**. À exécuter sans reconstruction concurrente des tables. Il ne modifie pas ces tables, mais il ne remplace pas une campagne CDC complète Oracle/SQL Server.

Les rapports de test sont produits sous `artifacts/test-reports/` lorsque les scripts aboutissent.

### Résultats documentés

| Niveau | Résultat | Portée |
|---|---:|---|
| Tests unitaires | **7 réussis** | Suite pytest isolée |
| Contrôles Kafka → Iceberg | **8 réussis** | Scénario d'intégration isolé avec événements de forme Debezium |
| Contrôles Spark/Iceberg | **3 réussis** | Smoke/contrat Iceberg |
| Intégrité Silver locale | **36 réussis** | Huit extensions, claims et tests de régression Oracle 3039 |

Le résultat **100 % de couverture** observé dans un rapport antérieur concerne un module de seulement **9 lignes**, et **non toute la plateforme**. La couverture doit être élargie avant toute affirmation sur la qualité globale du code.

### GitHub Actions et SonarQube

Le workflow `.github/workflows/fema-quality.yml` s'exécute sur les PR et push vers `master` et `develop`. Il définit :
- `unit-and-iceberg` ;
- `kafka-e2e` ;
- `sonarqube` (conditionnel).

L'analyse SonarQube Cloud requiert `SONAR_TOKEN` (secret), `SONAR_PROJECT_KEY` et `SONAR_ORGANIZATION` (variables). La présence du job ne garantit pas, à elle seule, qu'un Quality Gate a effectivement été exécuté et réussi.

## 9. Exploitation locale et restitution BI

### Prérequis

- WSL2 / Linux et Docker avec `docker compose` ;
- environnement Python adapté aux scripts exécutés ;
- image Spark 4.1 pour les traitements Iceberg ;
- données FEMA préparées et bases sources alimentées pour une reconstruction complète ;
- accès au warehouse Iceberg local **existant**.

Les scripts de test isolés ne nécessitent pas de reconstruire les deux bases sources. À l'inverse, le pipeline CDC complet nécessite des services source, des connecteurs et des topics correctement configurés.

### Rapport Power BI

L'export du portefeuille produit :

```text
exports/policy_portfolio.parquet
```

Dans le démonstrateur local vérifié, ce fichier contenait **117 437 polices**. Le rapport Power BI importe ce Parquet et peut exploiter :
- `canonical_policy_key` et `source_system` ;
- `canonical_holder_id` ;
- les primes, frais et couvertures ;
- les dates de prise d'effet et de résiliation ;
- les propriétés, typologies de bâtiment et indicateurs de risque.

Les **sinistres** et la **dimension titulaire** doivent être chargés comme tables distinctes pour le modèle relationnel final, en conservant respectivement `canonical_policy_key` et `canonical_holder_id` comme clés de jointure. Le fichier `policy_portfolio.parquet` ne représente pas, à lui seul, l'ensemble du modèle BI.

## 10. Infrastructure cible et prochaines étapes

Le projet vise une infrastructure **auto-administrée, portable et pilotable en infrastructure as code** :

```text
Terraform -> Kubernetes / services
                    ├── Strimzi / Kafka
                    ├── Kafka Connect / Debezium
                    ├── Spark Operator / traitements
                    ├── Ceph RGW ou stockage objet S3-compatible
                    ├── Catalogue Iceberg partagé
                    ├── Trino
                    ├── Airflow
                    └── Prometheus / Grafana
```

Étapes prioritaires :
1. finaliser les tables Gold au grain défini et leurs tests de non-duplication ;
2. finaliser le modèle Power BI (clients, polices, sinistres, finances, matrice des doublons) ;
3. installer un catalogue Iceberg compatible multi-moteur et raccorder Trino ;
4. définir un déploiement Terraform minimal sur GCP avec contrôle du coût et ressources démontables ;
5. ajouter les preuves de déploiement, de sécurité et d'observabilité ;
6. étendre les tests d'évolution de schéma, de replay, d'idempotence et de résilience.

**État actuel :** ces objectifs sont une **roadmap**, pas une liste de services déjà opérationnels sur GCP. La configuration de stockage local en catalogue Hadoop doit être adaptée avant un déploiement partagé.

## 11. Gouvernance et sécurité

- Aucun secret réel, mot de passe, token ou fichier `.env` ne doit être commité.
- Les données de clients enrichies/synthétiques peuvent contenir des champs assimilables à des données personnelles : ne pas publier de dumps de bases ni de fichiers Parquet contenant des identifiants nominaux.
- Les identifiants source doivent être distingués des identifiants canoniques.
- Le Bronze conserve la provenance ; une suppression ou mise à jour CDC ne doit pas effacer silencieusement l'audit.
- Les environnements de tests, les volumes de données et les credentials doivent rester séparés de la production.
- Le stockage, le catalogue et les snapshots Iceberg sont des responsabilités distinctes ; une sauvegarde du bucket seul ne suffit pas à démontrer une restauration cohérente du catalogue.

### Branches et contributions

- `master` : branche principale du dépôt actuel ;
- `develop` : intégration ;
- `feature/<sujet>` : évolution fonctionnelle ;
- `fix/<sujet>` : correction ;
- `modeling/<sujet>` : modélisation ;
- `ingestion/<sujet>` : ingestion et connecteurs ;
- `docs/<sujet>` : documentation.

Les conventions ci-dessus décrivent une organisation recommandée ; les branches historiques existantes ne sont pas nécessairement toutes conformes. Les modifications de pipeline doivent passer par une PR avec tests adaptés.

## 12. Références et périmètre documentaire

- [FEMA OpenFEMA](https://www.fema.gov/about/openfema) — données sources publiques NFIP ;
- [Apache Kafka](https://kafka.apache.org/documentation/) ;
- [Debezium](https://debezium.io/documentation/) ;
- [Apache Spark](https://spark.apache.org/docs/latest/) ;
- [Apache Iceberg](https://iceberg.apache.org/docs/latest/) ;
- [Trino](https://trino.io/docs/current/) ;
- [Terraform](https://developer.hashicorp.com/terraform/docs).

Documentation interne :
- `docs/decisions.md` — décisions d'architecture ;
- `docs/testing/STRATEGIE_DE_TESTS_BLOC2.md` — stratégie de validation ;
- `README-INSTALL.md`, `README-PHASE2.md`, `README-START-HERE.md` — procédures relatives aux campagnes CI ;
- DDL, mappings et scripts de test sous `data/source_system/`, `data_platform/` et `infrastructure/`.

---

**Projet de démonstration Data Engineering · FEMA NFIP · AbAssurance × AssurePlus.** Les résultats de tests présentés sont circonscrits aux scénarios et instantanés explicitement mentionnés.

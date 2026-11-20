# Stratégie de tests et CI/CD — FEMA Data Platform (Bloc 2)

## 1. Objectif et périmètre
Deux sources synthétiques dérivées de FEMA NFIP (SQL Server et Oracle),
capturées par Debezium / OpenLogReplicator, Kafka, Spark et Iceberg.
Objectif : démontrer fiabilité, reproductibilité et non-régression du CDC.
L'actuel stockage HadoopCatalog local est un socle de développement ; Ceph RGW
est une exigence d'architecture et fera l'objet d'un test séparé.

## 2. Politique d'exécution
- Pull request/push : lint + tests unitaires pytest + smoke Spark/Iceberg éphémère.
- Avant livraison : tests intégration réelle 25 topics/15 tables, Oracle et SQL Server,
  CDC INSERT/UPDATE/DELETE, qualité Silver et test Ceph.
- Chaque exécution crée son propre projet Compose, son réseau et son volume test.
- Aucun volume de production partagé ; aucun lien au réseau `fema-cdc`.
- Les résultats sont écrits sous `artifacts/test-reports/` **avant** `down -v`.
- Le shell trap déclenche le nettoyage même en cas de défaut ; un arrêt brutal du
  runner peut nécessiter un nettoyage manuel ultérieur.

## 3. Critères d'acceptation proposés
- 100 % des tests critiques en PASS ; aucun orphan sur les 8 tables liées à Policy.
- Unicité et non-nullité des clés canoniques ; 0 régression `ORACLE:3039`.
- Vérifier la dernière valeur `POLICY_COST=348.0000` (cas de non-régression).
- New code coverage >= 80 % sur logique de transformation **effectivement exécutée**.
- Aucun problème bloquant sur la Quality Gate SonarQube (une fois activée).
- Test end-to-end Oracle et SQL Server sur un stack de test isolé avant livraison.

## 4. Niveaux de test et preuves
| Niveau | Cible | Preuve |
|---|---|---|
| Unit | Réduction CDC, delete, ID Oracle, type mapping | pytest JUnit/coverage |
| Contrat d'intégration | Spark 4.1 + Iceberg 1.12, write/read/joins | spark-contract.json |
| Intégration réelle | Silver 15 tables, 8 relations 1:1 + claims 1:N | CSV/JUnit/logs à ajouter |
| E2E | Oracle/OLR/Debezium/Kafka/Spark/Iceberg/Ceph | Captures topic/offset, row count |
| Sécurité et qualité | Ruff, SonarQube, secrets et vulnérabilités | Job GitHub, Sonar Dashboard |

## 5. Incidents documentés
**INC-OLR-001 — Anomalie clé Oracle :** un événement Kafka a montré une valeur de
clé incohérente lors d'un test UPDATE. Le point exact de transformation doit
être démontré par preuves avant attribution définitive à OLR ou Debezium.
Test de régression requis : `POLICY_RECORD_ID` inchangé du SGBD à l'événement
et à la table Iceberg.

**INC-SIL-002 — Duplications de versions :** `ORACLE:3039` apparaissait quatre
fois dans `policy_terms` (offsets 54893/54986/54987/54988). Correctif :
filtre `ROW_NUMBER=1` au niveau du reducer CDC (après comparaison des offsets)
et exclusion des tombstones/deletes ; preuve : 54 986 lignes Oracle distinctes,
62 451 SQL Server ; valeur 348.0000 à l'offset 54988.

## 6. Limites du starter CI
Le smoke test fourni construit un **jeu synthétique réduit** et vérifie le
contrat Spark/Iceberg, pas le script réel `canonical_silver_extensions.py`.
Les unit tests s'exécutent sur une fonction de référence `cdc_contract.py`;
il faut encore intégrer cette logique à la transformation productive ou tester
directement le reducer PySpark de production. Ne pas présenter ces tests comme
une couverture de 100 % du pipeline réel.
Le stack complet Oracle/SQLServer/OLR/Debezium/Kafka/MinIO ou Ceph n'est pas
fourni ici : il doit être créé avec images, licences, secrets et fixtures de test.

## 7. Exécution
Depuis la racine du dépôt :
`bash infrastructure/testing/run_tests.sh`
Les rapports sont conservés dans `artifacts/test-reports/`; aucun autre chemin
persistant du dépôt ne doit être monté en écriture dans les conteneurs.

## 8. CI vs CD
CI automatise lint/test/analysis et bloque le merge si la Quality Gate échoue.
CD (phase suivante) déploiera le stack Terraform/K8s dans un environnement
éphémère, exécutera migrations+tests+smoke, et le détruira par `terraform destroy`
dans un job `always()` avec garde-fous budgétaires et noms isolés.

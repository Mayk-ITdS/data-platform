# FEMA CI phase 1 — izolowany Spark/Iceberg smoke test

## Zakres
To jest starter **CI, nie pelny test end-to-end Debezium/Kafka/Oracle/SQL Server**.
Uruchamia pytest+Ruff i niezalezny Spark 4.1/Iceberg 1.12 na syntetycznych danych.
Test sprawdza zapis/odczyt Iceberg, wybor najnowszego offsetu ORACLE:3039,
oraz brak osieroconych rekordow policy_terms. Nie uruchamia produkcyjnych ETL.

## Instalacja w repozytorium `~/projects/data-platform`
Rozpakuj ZIP **do katalogu glownego repo**. Nie nadpisuje istniejacych
`bronze_ingestion.py`, `canonical_*`, Compose produkcyjnego ani danych.
Nowe pliki istnieja pod `.github/workflows/`, `infrastructure/testing/`,
`data_platform/tests/`, `data_platform/spark/cdc_contract.py`, `docs/testing/`.
Nie zastępuj `.gitignore`: zawartość `.gitignore.append` możesz dopisać ręcznie.

    chmod +x infrastructure/testing/run_tests.sh
    bash infrastructure/testing/run_tests.sh

Wyniki: `artifacts/test-reports/{unit-junit.xml,coverage.xml,spark-contract.json,spark-smoke-junit.xml}`.
Zadne produkcyjne kontenery ani wolumeny nie sa zatrzymywane/usuwane.
`docker compose down --volumes` dotyczy wylacznie losowego projektu `fematest...`.
Dwa testowe serwisy wymagaja dostepu do internetu przy pierwszym uruchomieniu.

## GitHub Actions / SonarQube
Workflow uruchamia sie na PR i push do `master`/`develop`.
SonarQube Cloud jest opcjonalny: dodaj GitHub secret `SONAR_TOKEN` oraz
variables `SONAR_PROJECT_KEY`, `SONAR_ORGANIZATION`. Zeskanuj konfiguracje
przed wlaczeniem Quality Gate. Bez skonfigurowanych variables job Sonar jest pomijany.

## Ograniczenia
1. `cdc_contract.py` to czysta funkcja referencyjna, a nie funkcja wywolywana
   przez produkcyjny job Spark.
2. Przetwarzanie raw Debezium i przeplyw Kafka -> Bronze -> Silver wymagaja
   odrebnych fixture tests i osobnego Compose z CDC.
3. Test nie wykorzystuje prawdziwych plikow z `infrastructure/volumes/`.
4. Nie deklaruj testu jako 100% E2E ani produkcyjnego quality gate.

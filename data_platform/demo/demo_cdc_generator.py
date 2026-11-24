"""CDC demo generator: cycle across every configured SQL Server and Oracle source table.

A SQL UPDATE setting a column to its existing value produces a database write
(and typically a Debezium CDC UPDATE), without changing business data. This
is a CDC transport smoke test, NOT a simulation of changed insurance metrics.

By default this script only validates schemas and prints a plan; --apply is
required to execute. It never changes primary/foreign key values.
"""
from __future__ import annotations

import argparse
import os
import signal
import time
from datetime import datetime, timezone

SQL_TABLES = {
    "Building": ("BuildingId", "BuildingCategory"),
    "Claims": ("ClaimId", None),
    "Holder": ("HolderId", "HolderType"),
    "HolderClassification": ("HolderClassificationId", "CoverageSignal"),
    "Policy": ("PolicyId", None),
    "PolicyRating": ("PolicyRatingId", "RateMethod"),
    "PolicyTerms": ("PolicyTermsId", "PolicyCost"),
    "PolicyUnit": ("PolicyUnitId", "PolicyId"),
    "PremiumFinancial": ("PremiumFinancialId", "FederalPolicyFee"),
    "PrivateClient": ("HolderId", "LoyaltyScore"),
    "Property": ("PropertyId", "ReportedCity"),
    "PropertyUsage": ("PropertyUsageId", "OccupancyCategory"),
    "Risks": ("RiskId", "RatedFloodZone"),
}
ORACLE_TABLES = {
    "CLAIM": ("CLAIM_ID", "BUILDING_DAMAGE_AMOUNT"),
    "HOLDER": ("HOLDER_CONTEXT", "HOLDER_TYPE"),
    "POLICY": ("POLICY_RECORD_ID", "GROUP_SIGNATURE"),
    "POLICY_BUILDING_RISK": ("POLICY_RECORD_ID", "BUILDING_REPLACEMENT_COST"),
    "POLICY_BUILDING_TECHNICAL": ("POLICY_RECORD_ID", "BUILDING_CATEGORY"),
    "POLICY_CONTRACT": ("POLICY_RECORD_ID", "POLICY_COST"),
    "POLICY_HOLDER_CLASSIFICATION": ("POLICY_RECORD_ID", "COVERAGE_SIGNAL"),
    "POLICY_PREMIUM_FINANCIAL": ("POLICY_RECORD_ID", "FEDERAL_POLICY_FEE"),
    "POLICY_PROPERTY_LOCATION": ("POLICY_RECORD_ID", "REPORTED_CITY"),
    "POLICY_PROPERTY_USAGE": ("POLICY_RECORD_ID", "OCCUPANCY_CATEGORY"),
    "POLICY_RATING": ("POLICY_RECORD_ID", "RATE_METHOD"),
    "PRIVATE_CLIENT": ("HOLDER_CONTEXT", "STATE_CODE"),
}
STATEMENTS = {
    ('SQLSERVER', 'Building'): (
        'SELECT TOP (1) [BuildingId] FROM [dbo].[Building] ORDER BY [BuildingId]',
        'UPDATE [dbo].[Building] SET [BuildingCategory]=[BuildingCategory] WHERE [BuildingId]=?',
    ),
    ('SQLSERVER', 'Claims'): (
        'SELECT TOP (1) [ClaimId] FROM [dbo].[Claims] ORDER BY [ClaimId]',
        'UPDATE [dbo].[Claims] SET [ClaimId]=[ClaimId] WHERE [ClaimId]=?',
    ),
    ('SQLSERVER', 'Holder'): (
        'SELECT TOP (1) [HolderId] FROM [dbo].[Holder] ORDER BY [HolderId]',
        'UPDATE [dbo].[Holder] SET [HolderType]=[HolderType] WHERE [HolderId]=?',
    ),
    ('SQLSERVER', 'HolderClassification'): (
        'SELECT TOP (1) [HolderClassificationId] FROM [dbo].[HolderClassification] ORDER BY [HolderClassificationId]',
        'UPDATE [dbo].[HolderClassification] SET [CoverageSignal]=[CoverageSignal] WHERE [HolderClassificationId]=?',
    ),
    ('SQLSERVER', 'Policy'): (
        'SELECT TOP (1) [PolicyId] FROM [dbo].[Policy] ORDER BY [PolicyId]',
        'UPDATE [dbo].[Policy] SET [PolicyId]=[PolicyId] WHERE [PolicyId]=?',
    ),
    ('SQLSERVER', 'PolicyRating'): (
        'SELECT TOP (1) [PolicyRatingId] FROM [dbo].[PolicyRating] ORDER BY [PolicyRatingId]',
        'UPDATE [dbo].[PolicyRating] SET [RateMethod]=[RateMethod] WHERE [PolicyRatingId]=?',
    ),
    ('SQLSERVER', 'PolicyTerms'): (
        'SELECT TOP (1) [PolicyTermsId] FROM [dbo].[PolicyTerms] ORDER BY [PolicyTermsId]',
        'UPDATE [dbo].[PolicyTerms] SET [PolicyCost]=[PolicyCost] WHERE [PolicyTermsId]=?',
    ),
    ('SQLSERVER', 'PolicyUnit'): (
        'SELECT TOP (1) [PolicyUnitId] FROM [dbo].[PolicyUnit] ORDER BY [PolicyUnitId]',
        'UPDATE [dbo].[PolicyUnit] SET [PolicyId]=[PolicyId] WHERE [PolicyUnitId]=?',
    ),
    ('SQLSERVER', 'PremiumFinancial'): (
        'SELECT TOP (1) [PremiumFinancialId] FROM [dbo].[PremiumFinancial] ORDER BY [PremiumFinancialId]',
        'UPDATE [dbo].[PremiumFinancial] SET [FederalPolicyFee]=[FederalPolicyFee] WHERE [PremiumFinancialId]=?',
    ),
    ('SQLSERVER', 'PrivateClient'): (
        'SELECT TOP (1) [HolderId] FROM [dbo].[PrivateClient] ORDER BY [HolderId]',
        'UPDATE [dbo].[PrivateClient] SET [LoyaltyScore]=[LoyaltyScore] WHERE [HolderId]=?',
    ),
    ('SQLSERVER', 'Property'): (
        'SELECT TOP (1) [PropertyId] FROM [dbo].[Property] ORDER BY [PropertyId]',
        'UPDATE [dbo].[Property] SET [ReportedCity]=[ReportedCity] WHERE [PropertyId]=?',
    ),
    ('SQLSERVER', 'PropertyUsage'): (
        'SELECT TOP (1) [PropertyUsageId] FROM [dbo].[PropertyUsage] ORDER BY [PropertyUsageId]',
        'UPDATE [dbo].[PropertyUsage] SET [OccupancyCategory]=[OccupancyCategory] WHERE [PropertyUsageId]=?',
    ),
    ('SQLSERVER', 'Risks'): (
        'SELECT TOP (1) [RiskId] FROM [dbo].[Risks] ORDER BY [RiskId]',
        'UPDATE [dbo].[Risks] SET [RatedFloodZone]=[RatedFloodZone] WHERE [RiskId]=?',
    ),
    ('ORACLE', 'CLAIM'): (
        'SELECT "CLAIM_ID" FROM "CLAIM" ORDER BY "CLAIM_ID" FETCH FIRST 1 ROW ONLY',
        'UPDATE "CLAIM" SET "BUILDING_DAMAGE_AMOUNT"="BUILDING_DAMAGE_AMOUNT" WHERE "CLAIM_ID"=:pk',
    ),
    ('ORACLE', 'HOLDER'): (
        'SELECT "HOLDER_CONTEXT" FROM "HOLDER" ORDER BY "HOLDER_CONTEXT" FETCH FIRST 1 ROW ONLY',
        'UPDATE "HOLDER" SET "HOLDER_TYPE"="HOLDER_TYPE" WHERE "HOLDER_CONTEXT"=:pk',
    ),
    ('ORACLE', 'POLICY'): (
        'SELECT "POLICY_RECORD_ID" FROM "POLICY" ORDER BY "POLICY_RECORD_ID" FETCH FIRST 1 ROW ONLY',
        'UPDATE "POLICY" SET "GROUP_SIGNATURE"="GROUP_SIGNATURE" WHERE "POLICY_RECORD_ID"=:pk',
    ),
    ('ORACLE', 'POLICY_BUILDING_RISK'): (
        'SELECT "POLICY_RECORD_ID" FROM "POLICY_BUILDING_RISK" ORDER BY "POLICY_RECORD_ID" FETCH FIRST 1 ROW ONLY',
        'UPDATE "POLICY_BUILDING_RISK" SET "BUILDING_REPLACEMENT_COST"="BUILDING_REPLACEMENT_COST" WHERE "POLICY_RECORD_ID"=:pk',
    ),
    ('ORACLE', 'POLICY_BUILDING_TECHNICAL'): (
        'SELECT "POLICY_RECORD_ID" FROM "POLICY_BUILDING_TECHNICAL" ORDER BY "POLICY_RECORD_ID" FETCH FIRST 1 ROW ONLY',
        'UPDATE "POLICY_BUILDING_TECHNICAL" SET "BUILDING_CATEGORY"="BUILDING_CATEGORY" WHERE "POLICY_RECORD_ID"=:pk',
    ),
    ('ORACLE', 'POLICY_CONTRACT'): (
        'SELECT "POLICY_RECORD_ID" FROM "POLICY_CONTRACT" ORDER BY "POLICY_RECORD_ID" FETCH FIRST 1 ROW ONLY',
        'UPDATE "POLICY_CONTRACT" SET "POLICY_COST"="POLICY_COST" WHERE "POLICY_RECORD_ID"=:pk',
    ),
    ('ORACLE', 'POLICY_HOLDER_CLASSIFICATION'): (
        'SELECT "POLICY_RECORD_ID" FROM "POLICY_HOLDER_CLASSIFICATION" ORDER BY "POLICY_RECORD_ID" FETCH FIRST 1 ROW ONLY',
        'UPDATE "POLICY_HOLDER_CLASSIFICATION" SET "COVERAGE_SIGNAL"="COVERAGE_SIGNAL" WHERE "POLICY_RECORD_ID"=:pk',
    ),
    ('ORACLE', 'POLICY_PREMIUM_FINANCIAL'): (
        'SELECT "POLICY_RECORD_ID" FROM "POLICY_PREMIUM_FINANCIAL" ORDER BY "POLICY_RECORD_ID" FETCH FIRST 1 ROW ONLY',
        'UPDATE "POLICY_PREMIUM_FINANCIAL" SET "FEDERAL_POLICY_FEE"="FEDERAL_POLICY_FEE" WHERE "POLICY_RECORD_ID"=:pk',
    ),
    ('ORACLE', 'POLICY_PROPERTY_LOCATION'): (
        'SELECT "POLICY_RECORD_ID" FROM "POLICY_PROPERTY_LOCATION" ORDER BY "POLICY_RECORD_ID" FETCH FIRST 1 ROW ONLY',
        'UPDATE "POLICY_PROPERTY_LOCATION" SET "REPORTED_CITY"="REPORTED_CITY" WHERE "POLICY_RECORD_ID"=:pk',
    ),
    ('ORACLE', 'POLICY_PROPERTY_USAGE'): (
        'SELECT "POLICY_RECORD_ID" FROM "POLICY_PROPERTY_USAGE" ORDER BY "POLICY_RECORD_ID" FETCH FIRST 1 ROW ONLY',
        'UPDATE "POLICY_PROPERTY_USAGE" SET "OCCUPANCY_CATEGORY"="OCCUPANCY_CATEGORY" WHERE "POLICY_RECORD_ID"=:pk',
    ),
    ('ORACLE', 'POLICY_RATING'): (
        'SELECT "POLICY_RECORD_ID" FROM "POLICY_RATING" ORDER BY "POLICY_RECORD_ID" FETCH FIRST 1 ROW ONLY',
        'UPDATE "POLICY_RATING" SET "RATE_METHOD"="RATE_METHOD" WHERE "POLICY_RECORD_ID"=:pk',
    ),
    ('ORACLE', 'PRIVATE_CLIENT'): (
        'SELECT "HOLDER_CONTEXT" FROM "PRIVATE_CLIENT" ORDER BY "HOLDER_CONTEXT" FETCH FIRST 1 ROW ONLY',
        'UPDATE "PRIVATE_CLIENT" SET "STATE_CODE"="STATE_CODE" WHERE "HOLDER_CONTEXT"=:pk',
    ),
}
HALT = False


def stop(_signal, _frame):
    global HALT
    HALT = True


def required(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable missing: {name}")
    return value


def connect_sql():
    import pyodbc
    driver = os.getenv("FEMA_SQL_DRIVER", "ODBC Driver 18 for SQL Server")
    server = os.getenv("FEMA_SQL_SERVER", "localhost,1433")
    database = os.getenv("FEMA_SQL_DATABASE", "assurances")
    return pyodbc.connect(
        f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
        "Encrypt=yes;TrustServerCertificate=yes;",
        UID=os.getenv("FEMA_SQL_USER", "sa"),
        PWD=required("FEMA_SQL_PASSWORD"),
        autocommit=False,
        timeout=10,
    )


def connect_oracle():
    import oracledb
    return oracledb.connect(
        user=os.getenv("FEMA_ORACLE_USER", "ASSUREPLUS"),
        password=required("FEMA_ORACLE_PASSWORD"),
        dsn=os.getenv("FEMA_ORACLE_DSN", "localhost:1521/FREEPDB1"),
    )


def sql_columns(cur, table):
    cur.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?", table
    )
    return {row[0].lower() for row in cur.fetchall()}


def oracle_columns(cur, table):
    cur.execute(
        "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME=:name",
        {"name": table},
    )
    return {row[0].lower() for row in cur.fetchall()}


def get_plan(conn, tables, system):
    cur = conn.cursor()
    plan = []
    for table, (pk, preferred) in tables.items():
        columns = sql_columns(cur, table) if system == "SQLSERVER" else oracle_columns(cur, table)
        if not columns:
            raise RuntimeError(f"{system}.{table}: table missing or not accessible")
        if pk.lower() not in columns:
            raise RuntimeError(f"{system}.{table}: missing primary key column {pk}")
        if preferred is not None and preferred.lower() not in columns:
            raise RuntimeError(f"{system}.{table}: expected column missing: {preferred}")
        column = preferred or pk
        cur.execute(STATEMENTS[system, table][0])
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"{system}.{table}: empty table; cannot generate CDC UPDATE")
        plan.append((table, pk, column, row[0]))
    conn.rollback()
    return plan


def touch(conn, system, item):
    table, pk, column, key = item
    cur = conn.cursor()
    statement = STATEMENTS[system, table][1]
    cur.execute(statement, key if system == "SQLSERVER" else {"pk": key})
    if cur.rowcount != 1:
        conn.rollback()
        raise RuntimeError(f"{system}.{table}: expected 1 affected row, got {cur.rowcount}")
    conn.commit()
    return table, key


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Execute UPDATE transactions (default: dry-run)")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between cycles")
    parser.add_argument("--cycles", type=int, default=0, help="0 means run until Ctrl+C")
    args = parser.parse_args()
    if args.interval < 0.2 or args.cycles < 0:
        parser.error("interval must be >= 0.2; cycles must be >= 0")
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    sql = connect_sql()
    ora = connect_oracle()
    try:
        sql_plan = get_plan(sql, SQL_TABLES, "SQLSERVER")
        ora_plan = get_plan(ora, ORACLE_TABLES, "ORACLE")
        print(f"Schema validation OK: SQL Server {len(sql_plan)}, Oracle {len(ora_plan)}", flush=True)
        for system, plan in (("SQLSERVER", sql_plan), ("ORACLE", ora_plan)):
            for table, pk, column, key in plan:
                print(f"  {system}.{table}: PK={pk}; touch={column}; row={key}", flush=True)
        if not args.apply:
            print("DRY RUN: no UPDATE executed. Add --apply to run CDC smoke test.", flush=True)
            return

        print("START: two UPDATE transactions every interval; Ctrl+C to stop.", flush=True)
        iteration = 0
        while not HALT and (args.cycles == 0 or iteration < args.cycles):
            started = time.monotonic()
            for system, conn, plan in (
                ("SQLSERVER", sql, sql_plan), ("ORACLE", ora, ora_plan)
            ):
                item = plan[iteration % len(plan)]
                table, key = touch(conn, system, item)
                print(
                    f"{datetime.now(timezone.utc).isoformat()} "
                    f"cycle={iteration + 1} {system}.{table} key={key} COMMIT",
                    flush=True,
                )
            iteration += 1
            remaining = args.interval - (time.monotonic() - started)
            if remaining > 0 and not HALT:
                time.sleep(remaining)
        print(f"STOP: {iteration} cycles completed", flush=True)
    finally:
        sql.close()
        ora.close()


if __name__ == "__main__":
    main()

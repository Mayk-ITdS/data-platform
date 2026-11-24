"""Build a portable Power BI project from the current Gold snapshot."""
import json
from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path("/lakehouse/video/FEMA")
SCHEMAS = "https://developer.microsoft.com/json-schemas/fabric/item"


def write(path, data):
    dest = ROOT / path
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def build(spark):
    tables = []
    visuals = []
    for index, (table, label, columns) in enumerate([
        ("entity_counts", "Entités Silver", ["entity", "source_system", "record_count"]),
        ("cdc_topic_activity", "Activité Kafka", ["topic", "source_system", "event_count"]),
    ]):
        rows = spark.table("lakehouse.gold." + table).select(*columns).orderBy(*columns[:2]).collect()
        if not rows:
            raise ValueError("Empty Gold mart: " + table)
        # Inline the small aggregate snapshot so the report can reopen offline.
        import base64
        payload = base64.b64encode(json.dumps([list(r) for r in rows]).encode()).decode()
        names = ", ".join('"' + name + '"' for name in columns)
        expression = f'Table.FromRows(Json.Document(Binary.FromText("{payload}", BinaryEncoding.Base64)), {{{names}}})'
        tables.append({"name": table, "columns": [
            {"name": c, "sourceColumn": c, "dataType": "int64" if c == columns[-1] else "string",
             "summarizeBy": "none"} for c in columns],
            "partitions": [{"name": table, "mode": "import", "source": {"type": "m", "expression": expression}}]})
        projections = [{"field": {"Column": {"Expression": {"SourceRef": {"Entity": table}},
                         "Property": c}}, "queryRef": table + "." + c} for c in columns]
        visual = {"$schema": SCHEMAS + "/report/definition/visualContainer/2.0.0/schema.json",
                  "name": table, "position": {"x": 16 + 630*index, "y": 20, "z": index,
                  "height": 675, "width": 610, "tabOrder": index},
                  "visual": {"visualType": "tableEx", "query": {"queryState": {
                  "Values": {"projections": projections}}}, "visualContainerObjects": {"title": [
                  {"properties": {"show": {"expr": {"Literal": {"Value": "true"}}},
                  "text": {"expr": {"Literal": {"Value": "'" + label + "'"}}}}}]}}}
        visuals.append((table, visual))
    write("FEMA.SemanticModel/model.bim", {"name": "FEMA", "compatibilityLevel": 1567,
          "model": {"culture": "fr-FR", "defaultPowerBIDataSourceVersion": "powerBI_V3", "tables": tables}})
    write("FEMA.SemanticModel/definition.pbism", {"version": "1.0", "settings": {}})
    write("FEMA.Report/definition.pbir", {"version": "4.0", "datasetReference": {"byPath": {"path": "../FEMA.SemanticModel"}}})
    base = "FEMA.Report/definition/"
    write(base + "version.json", {"version": "2.0.0", "$schema": SCHEMAS + "/report/definition/versionMetadata/1.0.0/schema.json"})
    write(base + "report.json", {"$schema": SCHEMAS + "/report/definition/report/2.0.0/schema.json",
          "themeCollection": {"baseTheme": {"name": "CY24SU06", "reportVersionAtImport": "5.55",
          "type": "SharedResources"}}})
    write(base + "pages/pages.json", {"$schema": SCHEMAS + "/report/definition/pagesMetadata/1.0.0/schema.json",
          "pageOrder": ["Gold"], "activePageName": "Gold"})
    write(base + "pages/Gold/page.json", {"$schema": SCHEMAS + "/report/definition/page/2.0.0/schema.json",
          "name": "Gold", "displayName": "FEMA | Portefeuille et CDC", "displayOption": "FitToPage",
          "width": 1280, "height": 720})
    for name, visual in visuals:
        write(base + "pages/Gold/visuals/" + name + "/visual.json", visual)
    write("FEMA.pbip", {"version": "1.0", "artifacts": [{"report": {"path": "FEMA.Report"}}],
          "settings": {"enableAutoRecovery": True}})
    print("Power BI project:", ROOT / "FEMA.pbip", flush=True)


if __name__ == "__main__":
    session = SparkSession.builder.getOrCreate()
    try:
        build(session)
    finally:
        session.stop()

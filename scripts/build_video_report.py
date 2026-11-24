"""Generate the FEMA business analytics Power BI project."""

import base64
import json
import os
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(os.getenv("FEMA_REPORT_DIR", "/lakehouse/video/FEMA"))
SCHEMAS = "https://developer.microsoft.com/json-schemas/fabric/item"
BASE = "FEMA.Report/definition/"

MARTS = {
    "portfolio_summary": {
        "source_system": "string",
        "policy_count": "int64",
        "total_policy_cost": "double",
        "total_premium": "double",
        "building_coverage": "double",
        "contents_coverage": "double",
        "claim_count": "int64",
        "policies_with_claims": "int64",
    },
    "premium_analysis": {
        "source_system": "string",
        "policy_year": "int64",
        "policy_count": "int64",
        "total_premium": "double",
        "average_premium": "double",
        "total_federal_fee": "double",
        "total_hfiaa_surcharge": "double",
        "total_full_risk_premium": "double",
    },
    "claims_analysis": {
        "source_system": "string",
        "loss_year": "int64",
        "loss_month": "int64",
        "claim_count": "int64",
        "affected_policies": "int64",
    },
    "geographic_risk": {
        "source_system": "string",
        "property_state": "string",
        "fema_region": "string",
        "rated_flood_zone": "string",
        "policy_count": "int64",
        "claim_count": "int64",
        "total_building_replacement_cost": "double",
    },
    "customer_segmentation": {
        "source_system": "string",
        "customer_segment": "string",
        "customer_count": "int64",
        "policy_count": "int64",
        "average_loyalty_score": "double",
        "scored_customers": "int64",
    },
    "entity_counts": {
        "entity": "string",
        "source_system": "string",
        "record_count": "int64",
    },
    "cdc_topic_activity": {
        "topic": "string",
        "source_system": "string",
        "event_count": "int64",
    },
}

MEASURES = {
    "portfolio_summary": [
        ("Contrats", "SUM(portfolio_summary[policy_count])", "#,0"),
        ("Primes", "SUM(portfolio_summary[total_premium])", "$#,0"),
        ("Sinistres", "SUM(portfolio_summary[claim_count])", "#,0"),
        (
            "Taux de sinistralité",
            "DIVIDE(SUM(portfolio_summary[policies_with_claims]), "
            "SUM(portfolio_summary[policy_count]))",
            "0.00%",
        ),
        (
            "Couverture bâtiments",
            "SUM(portfolio_summary[building_coverage])",
            "$#,0",
        ),
        (
            "Couverture contenus",
            "SUM(portfolio_summary[contents_coverage])",
            "$#,0",
        ),
    ],
    "premium_analysis": [
        ("Primes annuelles", "SUM(premium_analysis[total_premium])", "$#,0"),
        ("Frais fédéraux", "SUM(premium_analysis[total_federal_fee])", "$#,0"),
    ],
    "claims_analysis": [
        ("Nombre de sinistres", "SUM(claims_analysis[claim_count])", "#,0"),
    ],
    "geographic_risk": [
        ("Contrats exposés", "SUM(geographic_risk[policy_count])", "#,0"),
        ("Sinistres par zone", "SUM(geographic_risk[claim_count])", "#,0"),
        (
            "Valeur de reconstruction",
            "SUM(geographic_risk[total_building_replacement_cost])",
            "$#,0",
        ),
    ],
    "customer_segmentation": [
        ("Clients", "SUM(customer_segmentation[customer_count])", "#,0"),
        (
            "Indice de fidélité",
            "DIVIDE(SUMX(customer_segmentation, "
            "customer_segmentation[average_loyalty_score] * "
            "customer_segmentation[scored_customers]), "
            "SUM(customer_segmentation[scored_customers]))",
            "0.00",
        ),
        (
            "Couverture du scoring",
            "DIVIDE(SUM(customer_segmentation[scored_customers]), "
            "SUM(customer_segmentation[customer_count]))",
            "0.00%",
        ),
    ],
    "entity_counts": [
        ("Enregistrements Silver", "SUM(entity_counts[record_count])", "#,0"),
    ],
    "cdc_topic_activity": [
        ("Événements CDC", "SUM(cdc_topic_activity[event_count])", "#,0"),
    ],
}


def write(path, data):
    dest = ROOT / path
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def model_table(name, columns, rows):
    payload = base64.b64encode(
        json.dumps(rows, ensure_ascii=False).encode("utf-8")
    ).decode("ascii")

    names = ", ".join(f'"{c}"' for c in columns)

    m_types = {
        "string": "type text",
        "int64": "Int64.Type",
        "double": "type number",
    }

    types = ", ".join(
        f'{{"{c}", {m_types[t]}}}'
        for c, t in columns.items()
    )

    expression = (
        "let "
        f'Source = Table.FromRows(Json.Document(Binary.FromText('
        f'"{payload}", BinaryEncoding.Base64)), {{{names}}}), '
        f'Typed = Table.TransformColumnTypes(Source, {{{types}}}) '
        "in Typed"
    )

    return {
        "name": name,
        "columns": [
            {
                "name": c,
                "sourceColumn": c,
                "dataType": t,
                "summarizeBy": "none",
            }
            for c, t in columns.items()
        ],
        "measures": [
            {
                "name": label,
                "expression": dax,
                "formatString": fmt,
            }
            for label, dax, fmt in MEASURES.get(name, [])
        ],
        "partitions": [
            {
                "name": name,
                "mode": "import",
                "source": {
                    "type": "m",
                    "expression": expression,
                },
            }
        ],
    }


def projection(table, field, measure=False):
    kind = "Measure" if measure else "Column"

    return {
        "field": {
            kind: {
                "Expression": {
                    "SourceRef": {"Entity": table}
                },
                "Property": field,
            }
        },
        "queryRef": f"{table}.{field}",
        "nativeQueryRef": {
            'source_system': 'Entreprise',
            'policy_year': 'Année',
            'loss_year': 'Année',
            'loss_month': 'Mois',
            'fema_region': 'Région FEMA',
            'rated_flood_zone': 'Zone inondable',
            'customer_segment': 'Segment',
            'entity': 'Entité',
            'topic': 'Topic Kafka',
        }.get(field, field),
    }


def visual(page, name, kind, title, position, roles):
    x, y, width, height = position

    query = {
        role: {
            "projections": [
                projection(*field) for field in fields
            ]
        }
        for role, fields in roles.items()
    }

    data = {
        "$schema": (
            SCHEMAS
            + "/report/definition/visualContainer/2.0.0/schema.json"
        ),
        "name": name,
        "position": {
            "x": x,
            "y": y,
            "z": 0,
            "width": width,
            "height": height,
            "tabOrder": 0,
        },
        "visual": {
            "visualType": kind,
            "query": {"queryState": query},
            "visualContainerObjects": {
                "title": [{
                    "properties": {
                        "show": {
                            "expr": {"Literal": {"Value": "true"}}
                        },
                        "text": {
                            "expr": {
                                "Literal": {
                                    "Value": "'" + title.replace("'", "''") + "'"
                                }
                            }
                        },
                    }
                }]
            },
        },
    }

    if kind == "card":
        data["visual"]["objects"] = {
            "categoryLabels": [{
                "properties": {
                    "show": {
                        "expr": {"Literal": {"Value": "false"}}
                    }
                }
            }]
        }

    if kind == "slicer":
        data["visual"]["objects"] = {
            "header": [{
                "properties": {
                    "show": {
                        "expr": {"Literal": {"Value": "false"}}
                    }
                }
            }],
            "data": [{
                "properties": {
                    "mode": {
                        "expr": {"Literal": {"Value": "'Tile'"}}
                    }
                }
            }],
            "general": [{
                "properties": {
                    "orientation": {
                        "expr": {"Literal": {"Value": "1D"}}
                    }
                }
            }]
        }

    if kind in {
        "donutChart",
        "clusteredColumnChart",
        "clusteredBarChart",
        "lineChart",
    }:
        objects = data["visual"].setdefault("objects", {})
        objects["dataPoint"] = [
            {
                "properties": {
                    "fill": {
                        "solid": {
                            "color": {
                                "expr": {
                                    "Literal": {"Value": "'#17365D'"}
                                }
                            }
                        }
                    }
                }
            }
        ]

    write(
        BASE + f"pages/{page}/visuals/{name}/visual.json",
        data,
    )


def card(page, name, table, measure, x):
    visual(
        page, name, "card", measure,
        (x, 15, 300, 110),
        {"Values": [(table, measure, True)]},
    )


def chart(
    page, name, kind, title, table,
    category, measure, position, legend=None,
):
    roles = {
        "Category": [(table, category)],
        "Y": [(table, measure, True)],
    }

    if legend:
        roles["Series"] = [(table, legend)]

    visual(page, name, kind, title, position, roles)


def slicer(page, table):
    visual(
        page, "assureur", "slicer", "Entreprise",
        (15, 130, 500, 75),
        {"Values": [(table, "source_system")]},
    )


def build_pages():
    pages = {
        "Portfolio": "01 | Portefeuille",
        "Premium": "02 | Primes",
        "Claims": "03 | Sinistres",
        "Risk": "04 | Risques",
        "Customers": "05 | Fidélité",
        "Monitoring": "06 | Monitoring CDC",
    }

    for name, label in pages.items():
        write(BASE + f"pages/{name}/page.json", {
            "$schema": SCHEMAS + "/report/definition/page/2.0.0/schema.json",
            "name": name,
            "displayName": label,
            "displayOption": "FitToPage",
            "width": 1280,
            "height": 720,
        })

    write(BASE + "pages/pages.json", {
        "$schema": SCHEMAS + "/report/definition/pagesMetadata/1.0.0/schema.json",
        "pageOrder": list(pages),
        "activePageName": "Portfolio",
    })

    # PAGE 1 — Portfolio
    p, t = "Portfolio", "portfolio_summary"

    card(p, "kpi_contracts", t, "Contrats", 15)
    card(p, "kpi_premiums", t, "Primes", 330)
    card(p, "kpi_claims", t, "Sinistres", 645)
    card(p, "kpi_rate", t, "Taux de sinistralité", 960)

    slicer(p, t)

    visual(
        p, "portfolio_donut", "donutChart",
        "Répartition du portefeuille",
        (15, 210, 610, 240),
        {
            "Category": [(t, "source_system")],
            "Y": [(t, "Contrats", True)],
        },
    )

    chart(
        p, "premium_bar", "clusteredColumnChart",
        "Primes par entreprise", t,
        "source_system", "Primes",
        (640, 210, 625, 240),
    )

    chart(
        p, "building_coverage", "clusteredBarChart",
        "Couverture des bâtiments", t,
        "source_system", "Couverture bâtiments",
        (15, 465, 610, 240),
    )

    chart(
        p, "claim_rate", "clusteredColumnChart",
        "Proportion des contrats avec sinistres", t,
        "source_system", "Taux de sinistralité",
        (640, 465, 625, 240),
    )

    # PAGE 2 — Premiums
    p, t = "Premium", "premium_analysis"

    card(p, "kpi_premium", t, "Primes annuelles", 15)
    card(p, "kpi_fees", t, "Frais fédéraux", 330)
    slicer(p, t)

    chart(
        p, "premium_trend", "lineChart",
        "Évolution annuelle des primes", t,
        "policy_year", "Primes annuelles",
        (15, 210, 1250, 240), "source_system",
    )

    chart(
        p, "federal_fees", "clusteredColumnChart",
        "Évolution des frais fédéraux", t,
        "policy_year", "Frais fédéraux",
        (15, 465, 1250, 240), "source_system",
    )

    # PAGE 3 — Claims
    p, t = "Claims", "claims_analysis"

    card(p, "kpi_claims", t, "Nombre de sinistres", 15)
    slicer(p, t)

    chart(
        p, "claims_trend", "lineChart",
        "Évolution annuelle des sinistres", t,
        "loss_year", "Nombre de sinistres",
        (15, 210, 1250, 240), "source_system",
    )

    chart(
        p, "seasonality", "clusteredColumnChart",
        "Saisonnalité mensuelle des sinistres", t,
        "loss_month", "Nombre de sinistres",
        (15, 465, 1250, 240), "source_system",
    )

    # PAGE 4 — Risk
    p, t = "Risk", "geographic_risk"

    card(p, "kpi_exposure", t, "Contrats exposés", 15)
    card(p, "kpi_value", t, "Valeur de reconstruction", 330)
    slicer(p, t)

    chart(
        p, "fema_regions", "clusteredColumnChart",
        "Exposition par région FEMA", t,
        "fema_region", "Contrats exposés",
        (15, 210, 610, 495), "source_system",
    )

    chart(
        p, "flood_zones", "clusteredBarChart",
        "Sinistres par zone inondable", t,
        "rated_flood_zone", "Sinistres par zone",
        (640, 210, 625, 495), "source_system",
    )

    # PAGE 5 — Customers
    p, t = "Customers", "customer_segmentation"

    card(p, "kpi_customers", t, "Clients", 15)
    card(p, "kpi_loyalty", t, "Indice de fidélité", 330)
    card(p, "kpi_scoring", t, "Couverture du scoring", 645)
    slicer(p, t)

    chart(
        p, "segments", "clusteredColumnChart",
        "Segmentation selon le nombre de contrats", t,
        "customer_segment", "Clients",
        (15, 210, 610, 495), "source_system",
    )

    chart(
        p, "loyalty", "lineChart",
        "Indice de fidélité par segment", t,
        "customer_segment", "Indice de fidélité",
        (640, 210, 625, 495), "source_system",
    )

    # PAGE 6 — Monitoring
    p = "Monitoring"

    card(p, "kpi_events", "cdc_topic_activity", "Événements CDC", 15)
    card(p, "kpi_silver", "entity_counts", "Enregistrements Silver", 330)

    chart(
        p, "silver_entities", "clusteredBarChart",
        "Volumétrie des entités Silver", "entity_counts",
        "entity", "Enregistrements Silver",
        (15, 145, 610, 560), "source_system",
    )

    chart(
        p, "kafka_topics", "clusteredBarChart",
        "Activité des topics Kafka", "cdc_topic_activity",
        "topic", "Événements CDC",
        (640, 145, 625, 560),
    )

    return pages


def build(spark):
    tables = []

    for name, columns in MARTS.items():
        df = spark.table(f"lakehouse.gold.{name}")
        df = df.select(*[
            df[c].cast(
                {"string": "string", "int64": "long", "double": "double"}[t]
            ).alias(c)
            for c, t in columns.items()
        ])

        rows = [list(row) for row in df.collect()]

        if not rows:
            raise ValueError(f"Empty Gold mart: {name}")

        for row in rows:
            if "source_system" in columns:
                index = list(columns).index("source_system")
                row[index] = {
                    "SQLSERVER": "AbAssurance",
                    "ORACLE": "AssurePlus",
                }.get(row[index], row[index])

        if name == "customer_segmentation":
            columns = {**columns, "segment_order": "int64"}
            segment_index = list(columns).index("customer_segment")

            segment_order = {
                "0": 0,
                "1": 1,
                "2-5": 2,
                "6-10": 3,
                "11+": 4,
            }

            for row in rows:
                row.append(segment_order[row[segment_index]])

        table = model_table(name, columns, rows)

        if name == "customer_segmentation":
            for column in table["columns"]:
                if column["name"] == "customer_segment":
                    column["sortByColumn"] = "segment_order"
                elif column["name"] == "segment_order":
                    column["isHidden"] = True

        tables.append(table)

        print(
            f"PASS: Gold mart {name} — {len(rows)} rows",
            flush=True,
        )

    write("FEMA.SemanticModel/model.bim", {
        "name": "FEMA",
        "compatibilityLevel": 1567,
        "model": {
            "culture": "fr-FR",
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "tables": tables,
        },
    })

    write("FEMA.SemanticModel/definition.pbism", {
        "version": "1.0",
        "settings": {},
    })

    write("FEMA.Report/definition.pbir", {
        "version": "4.0",
        "datasetReference": {
            "byPath": {"path": "../FEMA.SemanticModel"}
        },
    })

    write(BASE + "version.json", {
        "version": "2.0.0",
        "$schema": SCHEMAS + "/report/definition/versionMetadata/1.0.0/schema.json",
    })

    write(BASE + "report.json", {
        "$schema": SCHEMAS + "/report/definition/report/2.0.0/schema.json",
        "themeCollection": {
            "baseTheme": {
                "name": "CY24SU06",
                "reportVersionAtImport": "5.55",
                "type": "SharedResources",
            }
        },
    })

    pages = build_pages()

    write("FEMA.pbip", {
        "version": "1.0",
        "artifacts": [{"report": {"path": "FEMA.Report"}}],
        "settings": {"enableAutoRecovery": True},
    })

    print("=" * 70)
    print("POWER BI — RAPPORT ANALYTIQUE FEMA")
    print("=" * 70)

    for label in pages.values():
        print(f"PASS: {label}")

    print(f"Power BI project: {ROOT / 'FEMA.pbip'}")
    print("PASS: Report files generated", flush=True)


if __name__ == "__main__":
    session = (
        SparkSession.builder
        .appName("FEMA-Power-BI-Gold")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("WARN")

    try:
        build(session)
    finally:
        session.stop()

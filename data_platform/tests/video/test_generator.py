import sys
from unittest.mock import MagicMock

import pytest
from data_platform.demo import demo_cdc_generator as demo


@pytest.mark.parametrize("system,tables", [("SQLSERVER", demo.SQL_TABLES), ("ORACLE", demo.ORACLE_TABLES)])
def test_every_configured_statement(system, tables):
    conn = MagicMock()
    cur = conn.cursor.return_value
    cur.fetchall.side_effect = [[(pk,), (col or pk,)] for pk, col in tables.values()]
    cur.fetchone.return_value = (17,)
    cur.rowcount = 1
    plan = demo.get_plan(conn, tables, system)
    assert [item[0] for item in plan] == list(tables)
    for table, pk, col, key in plan:
        select, update = demo.STATEMENTS[system, table]
        assert table in select and pk in select and table in update and col in update
        assert ("?" if system == "SQLSERVER" else ":pk") in update
        assert demo.touch(conn, system, (table, pk, col, key)) == (table, 17)
        cur.execute.assert_called_with(update, 17 if system == "SQLSERVER" else {"pk": 17})
    assert conn.commit.call_count == len(tables)


@pytest.mark.parametrize("columns,row", [([], (1,)), ([("wrong",)], (1,)), ([("HolderId",)], (1,)),
                                          ([("HolderId",), ("HolderType",)], None)])
def test_invalid_source_stops_before_writes(columns, row):
    conn = MagicMock()
    conn.cursor.return_value.fetchall.return_value = columns
    conn.cursor.return_value.fetchone.return_value = row
    with pytest.raises(RuntimeError):
        demo.get_plan(conn, {"Holder": demo.SQL_TABLES["Holder"]}, "SQLSERVER")
    conn.commit.assert_not_called()


def test_bounds_and_unknown_tables():
    conn = MagicMock()
    conn.cursor.return_value.rowcount = 2
    with pytest.raises(RuntimeError):
        demo.touch(conn, "SQLSERVER", ("Holder", "HolderId", "HolderType", 1))
    conn.rollback.assert_called_once()
    with pytest.raises(KeyError):
        demo.touch(conn, "SQLSERVER", ("UnknownTable", "", "", 1))
    conn.commit.assert_not_called()


def test_connection_options(monkeypatch):
    sql, oracle = MagicMock(), MagicMock()
    monkeypatch.setitem(sys.modules, "pyodbc", sql)
    monkeypatch.setitem(sys.modules, "oracledb", oracle)
    for engine in ("SQL", "ORACLE"):
        monkeypatch.setenv("FEMA_" + engine + "_PASSWORD", "fixture")
    demo.connect_sql()
    demo.connect_oracle()
    assert sql.connect.call_args.kwargs["autocommit"] is False
    assert oracle.connect.call_args.kwargs["user"] == "ASSUREPLUS"
    monkeypatch.delenv("FEMA_SQL_PASSWORD")
    with pytest.raises(RuntimeError):
        demo.connect_sql()


@pytest.mark.parametrize("apply", [True, False])
def test_cli_cycles_and_closes(monkeypatch, apply):
    sql, ora, touch = MagicMock(), MagicMock(), MagicMock(return_value=("Table", 1))
    monkeypatch.setattr(demo, "connect_sql", lambda: sql)
    monkeypatch.setattr(demo, "connect_oracle", lambda: ora)
    monkeypatch.setattr(demo, "get_plan", lambda *args: [("Table", "Id", "Value", 1)])
    monkeypatch.setattr(demo, "touch", touch)
    monkeypatch.setattr(demo.signal, "signal", lambda *args: None)
    monkeypatch.setattr(demo.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(demo, "HALT", False)
    monkeypatch.setattr(sys, "argv", ["demo", "--cycles", "1"] + (["--apply"] if apply else []))
    demo.main()
    assert touch.call_count == (2 if apply else 0)
    sql.close.assert_called_once()
    ora.close.assert_called_once()
    demo.stop(None, None)
    assert demo.HALT
    monkeypatch.setattr(sys, "argv", ["demo", "--interval", "0"])
    with pytest.raises(SystemExit):
        demo.main()

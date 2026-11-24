from unittest.mock import MagicMock
import pytest
from data_platform.spark import bronze_ingestion as bronze


@pytest.mark.parametrize("offsets,exists,trigger,error", [
    ("invalid", True, "availableNow", ValueError), ("earliest", False, "availableNow", RuntimeError),
    ("earliest", True, "unsupported", ValueError), ("earliest", True, "availableNow", None),
    ("latest", True, "processingTime", None),
])
def test_stream_lifecycle(monkeypatch, offsets, exists, trigger, error):
    engine = MagicMock()
    engine.catalog.tableExists.return_value = exists
    builder = MagicMock()
    builder.appName.return_value = builder
    builder.config.return_value = builder
    builder.getOrCreate.return_value = engine
    monkeypatch.setattr(bronze, "SparkSession", MagicMock(builder=builder))
    monkeypatch.setattr(bronze, "F", MagicMock())
    monkeypatch.setattr(bronze, "STARTING_OFFSETS", offsets)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "fixture:9092")
    monkeypatch.setenv("BRONZE_TRIGGER", trigger)
    stream = engine.readStream.format.return_value
    stream.option.return_value = stream
    writer = stream.load.return_value.select.return_value.writeStream.format.return_value
    writer.outputMode.return_value = writer
    writer.option.return_value = writer
    writer.trigger.return_value = writer
    if error:
        with pytest.raises(error):
            bronze.main()
    else:
        bronze.main()
        writer.toTable.assert_called_once_with(bronze.BRONZE_TABLE)
        writer.toTable.return_value.awaitTermination.assert_called_once()
        assert writer.trigger.call_args.kwargs == ({"availableNow": True} if trigger == "availableNow" else {"processingTime": "30 seconds"})
        stream.option.assert_any_call("subscribe", ",".join(bronze.TOPICS))
        writer.option.assert_any_call("checkpointLocation", bronze.CHECKPOINT)
    if offsets != "invalid":
        engine.stop.assert_called_once()

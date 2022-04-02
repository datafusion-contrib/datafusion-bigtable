import pytest
from datafusion import ExecutionContext
from datafusion._internal import Table as DatafusionTable
from datafusion_bigtable import BigtableTable


@pytest.fixture
def ctx():
    return ExecutionContext()


@pytest.fixture
def df_table():
    bigtable = BigtableTable(
        project="emulator",
        instance="dev",
        table="weather_balloons",
        column_family="measurements",
        table_partition_cols=["region", "balloon_id", "event_minute"],
        table_partition_separator="#",
        qualifiers=["temperature"],
        only_read_latest=True,
    )
    return DatafusionTable(bigtable.table())


def test_sql(ctx, df_table):
    ctx.register_table("weather_balloons", df_table)
    result = ctx.sql(
        "SELECT region, balloon_id, event_minute, temperature, \"_timestamp\" FROM weather_balloons where region = 'us-west2' and balloon_id IN ('3698') and event_minute BETWEEN '2021-03-05-1200' AND '2021-03-05-1201' ORDER BY \"_timestamp\""
    ).collect()
    assert result.to_pydict() == {}

import os
import tempfile

import apache_beam as beam
import tensorflow as tf
from tensorflow_data_validation.api import stats_api
from tensorflow_data_validation.statistics import stats_options as options
from tensorflow_data_validation.utils import stats_util
import pyarrow as pa
from tfx_bsl.arrow.array_util import ToSingletonListArray


def generate_statistics_from_parquet(
    data_location: str, stats_options: options.StatsOptions = options.StatsOptions()
):
    output_path = os.path.join(tempfile.mkdtemp(), "data_stats.tfrecord")
    output_dir_path = os.path.dirname(output_path)
    if not tf.io.gfile.exists(output_dir_path):
        tf.io.gfile.makedirs(output_dir_path)

    def decode_parquet(table: pa.lib.Table) -> pa.lib.RecordBatch:

        return pa.RecordBatch.from_arrays(
            [ToSingletonListArray(col.chunks[0]) for col in table.columns],
            table.column_names,
        )

    with beam.Pipeline(options=None) as p:
        _ = (
            p
            | "ReadData" >> beam.io.parquetio.ReadFromParquetBatched(data_location)
            | "DecodeParquet" >> beam.Map(decode_parquet)
            | "GenerateStatistics" >> stats_api.GenerateStatistics(stats_options)
            | "WriteStatsOutput" >> stats_api.WriteStatisticsToTFRecord(output_path)
        )
    return stats_util.load_statistics(output_path)



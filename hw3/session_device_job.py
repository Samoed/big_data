from pyflink.common import SimpleStringSchema, Time
from pyflink.common.typeinfo import RowTypeInfo, Types
from pyflink.common.types import Row
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.checkpoint_storage import CheckpointStorage
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import MapFunction, ReduceFunction
from pyflink.datastream.window import (
    ProcessingTimeSessionWindows,
    TumblingProcessingTimeWindows,
)


class MaxTemperatureReduceFunction(ReduceFunction):
    def reduce(self, value1: Row, value2: Row):
        return value1 if value1.temperature > value2.temperature else value2


class TemperatureFunction(MapFunction):
    def map(self, value):
        device_id, temperature, execution_time = value
        return str(
            {
                "device_id": device_id,
                "temperature": temperature - 273,
                "execution_time": execution_time,
            }
        )


def python_data_stream_example():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Set the parallelism to be one to make sure that all data including fired timer and normal data
    # are processed by the same worker and the collected result would be in order which is good for
    # assertion.
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    # storage = CheckpointStorage(r"file:///opt/pyflink/tmp/checkpoints/logs")
    storage = CheckpointStorage(r"hdfs://namenode:8020/flink/checkpoints")
    env.get_checkpoint_config().set_checkpoint_storage(storage)
    env.enable_checkpointing(3000)

    type_info: RowTypeInfo = Types.ROW_NAMED(
        ["device_id", "temperature", "execution_time"],
        [Types.LONG(), Types.DOUBLE(), Types.INT()],
    )

    json_row_schema = (
        JsonRowDeserializationSchema.builder().type_info(type_info).build()
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("itmo2023")
        .set_group_id("pyflink-e2e-source")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(json_row_schema)
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("itmo2023_processed")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    (
        ds.key_by(lambda x: x[0])
        .window(ProcessingTimeSessionWindows.with_gap(Time.seconds(10)))
        .reduce(MaxTemperatureReduceFunction())
        .map(TemperatureFunction(), Types.STRING())
        .sink_to(sink)
    )

    env.execute_async("Session devices preprocessing")


if __name__ == "__main__":
    python_data_stream_example()

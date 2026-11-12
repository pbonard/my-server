package cdcjob.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * Passes only insert events ("op" = "c") to the wrapped schema.
 * Drops updates, deletes and tombstones, e.g. the outbox rows deleted right after their insert.
 */
public class InsertOnlyDeserializationSchema implements DebeziumDeserializationSchema<RowData> {

  private final DebeziumDeserializationSchema<RowData> delegate;

  public InsertOnlyDeserializationSchema(DebeziumDeserializationSchema<RowData> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void deserialize(SourceRecord record, Collector<RowData> out) throws Exception {
    if (!(record.value() instanceof Struct value) || value.schema().field("op") == null) {
      return;
    }
    if ("c".equals(value.getString("op"))) {
      delegate.deserialize(record, out);
    }
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return delegate.getProducedType();
  }
}

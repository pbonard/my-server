package cdcjob.source;

import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.cdc.connectors.vitess.VitessSource;
import org.apache.flink.cdc.connectors.vitess.config.SchemaAdjustmentMode;
import org.apache.flink.cdc.connectors.vitess.config.TabletType;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VitessOpTsTableSource implements ScanTableSource, SupportsReadingMetadata {

  private static final String OP_TS = "op_ts";

  private final DataType physicalDataType;
  private final String hostname;
  private final int port;
  private final String keyspace;
  private final String tableName;
  private final TabletType tabletType;
  private final String name;
  private final String username;
  private final String password;
  private final boolean insertOnly;

  private DataType producedDataType;
  private List<String> metadataKeys = List.of();

  public VitessOpTsTableSource(DataType physicalDataType, String hostname, int port, String keyspace,
                               String tableName, TabletType tabletType, String name,
                               String username, String password, boolean insertOnly) {
    this.physicalDataType = physicalDataType;
    this.producedDataType = physicalDataType;
    this.hostname = hostname;
    this.port = port;
    this.keyspace = keyspace;
    this.tableName = tableName;
    this.tabletType = tabletType;
    this.name = name;
    this.username = username;
    this.password = password;
    this.insertOnly = insertOnly;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return insertOnly ? ChangelogMode.insertOnly() : ChangelogMode.all();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
    DebeziumDeserializationSchema<RowData> deserializer = RowDataDebeziumDeserializeSchema.newBuilder()
        .setPhysicalRowType((RowType) physicalDataType.getLogicalType())
        .setMetadataConverters(metadataConverters())
        .setResultTypeInfo(context.createTypeInformation(producedDataType))
        .setServerTimeZone(ZoneId.of("UTC"))
        .build();

    DebeziumSourceFunction<RowData> sourceFunction = VitessSource.<RowData>builder()
        .hostname(hostname)
        .port(port)
        .keyspace(keyspace)
        .tableIncludeList(tableName)
        .username(username)
        .password(password)
        .tabletType(tabletType)
        // default of the 'vitess-cdc' factory, the builder's own default is NONE
        .schemaNameAdjustmentMode(SchemaAdjustmentMode.AVRO)
        .name(name)
        .deserializer(insertOnly ? new InsertOnlyDeserializationSchema(deserializer) : deserializer)
        .build();
    return SourceFunctionProvider.of(sourceFunction, false);
  }

  private MetadataConverter[] metadataConverters() {
    List<MetadataConverter> converters = new ArrayList<>();
    for (String key : metadataKeys) {
      if (OP_TS.equals(key)) {
        converters.add(record -> {
          //source have metadata of binlog which is retrieved by debezium
          Struct source = ((Struct) record.value()).getStruct("source");
          Long tsMs = source == null ? null : source.getInt64("ts_ms");
          return tsMs == null ? null : TimestampData.fromEpochMillis(tsMs);
        });
      }
    }
    return converters.toArray(new MetadataConverter[0]);
  }

  @Override
  public Map<String, DataType> listReadableMetadata() {
    return Map.of(OP_TS, DataTypes.TIMESTAMP_LTZ(3));
  }

  @Override
  public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
    this.metadataKeys = metadataKeys;
    this.producedDataType = producedDataType;
  }

  @Override
  public DynamicTableSource copy() {
    VitessOpTsTableSource copy = new VitessOpTsTableSource(physicalDataType, hostname, port, keyspace,
        tableName, tabletType, name, username, password, insertOnly);
    copy.metadataKeys = metadataKeys;
    copy.producedDataType = producedDataType;
    return copy;
  }

  @Override
  public String asSummaryString() {
    return "Vitess-CDC with op_ts";
  }
}

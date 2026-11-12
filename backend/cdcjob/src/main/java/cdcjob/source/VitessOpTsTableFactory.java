package cdcjob.source;

import org.apache.flink.cdc.connectors.vitess.config.TabletType;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

public class VitessOpTsTableFactory implements DynamicTableSourceFactory {

  public static final String IDENTIFIER = "vitess-cdc-op-ts";

  static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname").stringType().noDefaultValue();
  static final ConfigOption<Integer> PORT = ConfigOptions.key("port").intType().defaultValue(15991);
  static final ConfigOption<String> KEYSPACE = ConfigOptions.key("keyspace").stringType().noDefaultValue();
  static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table-name").stringType().noDefaultValue();
  static final ConfigOption<String> TABLET_TYPE = ConfigOptions.key("tablet-type").stringType().defaultValue("RDONLY");
  static final ConfigOption<String> NAME = ConfigOptions.key("name").stringType().defaultValue("flink");
  static final ConfigOption<String> USERNAME = ConfigOptions.key("username").stringType().noDefaultValue();
  static final ConfigOption<String> PASSWORD = ConfigOptions.key("password").stringType().noDefaultValue();
  static final ConfigOption<Boolean> INSERT_ONLY = ConfigOptions.key("insert-only").booleanType().defaultValue(false);

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();

    ReadableConfig options = helper.getOptions();
    return new VitessOpTsTableSource(
        context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType(),
        options.get(HOSTNAME),
        options.get(PORT),
        options.get(KEYSPACE),
        options.get(TABLE_NAME),
        TabletType.valueOf(options.get(TABLET_TYPE).toUpperCase()),
        options.get(NAME),
        options.getOptional(USERNAME).orElse(null),
        options.getOptional(PASSWORD).orElse(null),
        options.get(INSERT_ONLY));
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of(HOSTNAME, KEYSPACE, TABLE_NAME);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(PORT, TABLET_TYPE, NAME, USERNAME, PASSWORD, INSERT_ONLY);
  }
}

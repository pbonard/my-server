package cdcjob;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CdcJob {

  private static final String DEFAULT_SCRIPT = "/cdc.sql";
  private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([A-Z0-9_]+)}");

  public static void main(String[] args) throws Exception {
    Map<String, String> variables = buildVariables();

    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    tEnv.getConfig().set("pipeline.name", "CdcJob");
    // sources declare no primary key, without this the planner adds a stateful materializer before upsert sinks
    tEnv.getConfig().set("table.exec.sink.upsert-materialize", "NONE");

    StatementSet inserts = tEnv.createStatementSet();
    // split before substitution, PEM lines('-----BEGIN ...') would look like comments
    for (String rawStatement : split(readScript(args))) {
      String statement = substitute(rawStatement, variables);
      if (statement.regionMatches(true, 0, "INSERT", 0, 6)) {
        inserts.addInsertSql(statement);
      } else {
        tEnv.executeSql(statement);
      }
    }
    inserts.execute();
  }

  private static String readScript(String[] args) throws IOException {
    if (args.length > 0) {
      return Files.readString(Path.of(args[0]));
    }
    try (InputStream in = CdcJob.class.getResourceAsStream(DEFAULT_SCRIPT)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  // env variables + PEM contents of the kafka certificates, flink kafka connector takes PEM as a string
  private static Map<String, String> buildVariables() throws IOException {
    Map<String, String> variables = new HashMap<>(System.getenv());
    variables.put("KAFKA_CA_CERT", Files.readString(Path.of(System.getenv("KAFKA_CA_CERT_PATH"))));
    variables.put("KAFKA_USER_CERT", Files.readString(Path.of(System.getenv("KAFKA_USER_CERT_PATH"))));
    variables.put("KAFKA_USER_KEY", Files.readString(Path.of(System.getenv("KAFKA_USER_KEY_PATH"))));
    return variables;
  }

  private static String substitute(String script, Map<String, String> variables) {
    Matcher matcher = PLACEHOLDER.matcher(script);
    StringBuilder sb = new StringBuilder();
    while (matcher.find()) {
      String value = variables.get(matcher.group(1));
      if (value == null) {
        throw new IllegalArgumentException("missing variable: " + matcher.group(1));
      }
      matcher.appendReplacement(sb, Matcher.quoteReplacement(value));
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

  // statements end with ';' at the end of a line, '--' comment lines are dropped
  private static List<String> split(String script) {
    List<String> statements = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    for (String line : script.split("\n")) {
      if (line.strip().startsWith("--")) {
        continue;
      }
      current.append(line).append('\n');
      if (line.strip().endsWith(";")) {
        String statement = current.toString().strip();
        statements.add(statement.substring(0, statement.length() - 1));
        current.setLength(0);
      }
    }
    if (!current.toString().isBlank()) {
      statements.add(current.toString().strip());
    }
    return statements;
  }
}

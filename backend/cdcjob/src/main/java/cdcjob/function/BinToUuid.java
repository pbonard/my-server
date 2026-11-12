package cdcjob.function;

import org.apache.flink.table.functions.ScalarFunction;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * BINARY(16) id -> UUID string, same as mysql BIN_TO_UUID.
 */
public class BinToUuid extends ScalarFunction {

  public String eval(byte[] bytes) {
    if (bytes == null || bytes.length != 16) {
      return null;
    }
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    return new UUID(buffer.getLong(), buffer.getLong()).toString();
  }
}

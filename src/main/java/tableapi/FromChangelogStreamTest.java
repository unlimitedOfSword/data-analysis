package tableapi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public class FromChangelogStreamTest {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
// === EXAMPLE 1 ===

// interpret the stream as a retract stream

// create a changelog DataStream
        DataStream<Row> dataStream =
                env.fromElements(
                        Row.ofKind(RowKind.INSERT, "Alice", 12),
                        Row.ofKind(RowKind.INSERT, "Bob", 5),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));

// interpret the DataStream as a Table
        Table table = tableEnv.fromChangelogStream(dataStream);

// register the table under a name and perform an aggregation
        tableEnv.createTemporaryView("InputTable", table);
        tableEnv.executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
                .print();


// === EXAMPLE 2 ===

// interpret the stream as an upsert stream (without a need for UPDATE_BEFORE)

// create a changelog DataStream
        DataStream<Row> dataStream1 =
                env.fromElements(
                        Row.ofKind(RowKind.INSERT, "Alice", 12),
                        Row.ofKind(RowKind.INSERT, "Bob", 5),
                        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));

// interpret the DataStream as a Table
        Table table1 =
                tableEnv.fromChangelogStream(
                        dataStream1,
                        Schema.newBuilder().primaryKey("f0").build(),
                        ChangelogMode.upsert());

// register the table under a name and perform an aggregation
        tableEnv.createTemporaryView("InputTable1", table1);
        tableEnv
                .executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable1 GROUP BY f0")
                .print();
    }
}

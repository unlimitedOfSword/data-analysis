package tableapi;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CreateTemporaryViewTest {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
// create some DataStream
        DataStream<Tuple2<Long, String>> dataStream = env.fromElements(
                Tuple2.of(12L, "Alice"),
                Tuple2.of(0L, "Bob"));


// === EXAMPLE 1 ===

// register the DataStream as view "MyView" in the current session
// all columns are derived automatically

        tableEnv.createTemporaryView("MyView", dataStream);

        tableEnv.from("MyView").printSchema();

// prints:
// (
//  `f0` BIGINT NOT NULL,
//  `f1` STRING
// )


// === EXAMPLE 2 ===

// register the DataStream as view "MyView" in the current session,
// provide a schema to adjust the columns similar to `fromDataStream`

// in this example, the derived NOT NULL information has been removed

        tableEnv.createTemporaryView(
                "MyView1",
                dataStream,
                Schema.newBuilder()
                        .column("f0", "BIGINT")
                        .column("f1", "STRING")
                        .build());

        tableEnv.from("MyView1").printSchema();

// prints:
// (
//  `f0` BIGINT,
//  `f1` STRING
// )


// === EXAMPLE 3 ===

// use the Table API before creating the view if it is only about renaming columns

        tableEnv.createTemporaryView(
                "MyView2",
                tableEnv.fromDataStream(dataStream).as("id", "name"));

        tableEnv.from("MyView2").printSchema();
    }
}

package tableapi;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.*;
public class PipelinesToDataStreamAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        StreamStatementSet statementSet = tableEnv.createStatementSet();

// create some source
        TableDescriptor sourceDescriptor =
                TableDescriptor.forConnector("datagen")
                        .option("number-of-rows", "4")
                        .schema(
                                Schema.newBuilder()
                                        .column("myCol", DataTypes.INT())
                                        .column("myOtherCol", DataTypes.BOOLEAN())
                                        .build())
                        .build();

// create some sink
        TableDescriptor sinkDescriptor = TableDescriptor.forConnector("print").build();

// add a pure Table API pipeline
        Table tableFromSource = tableEnv.from(sourceDescriptor);
        statementSet.add(tableFromSource.insertInto(sinkDescriptor));

// use table sinks for the DataStream API pipeline
        DataStream<Integer> dataStream = env.fromElements(1, 2, 3);
        Table tableFromStream = tableEnv.fromDataStream(dataStream);
        statementSet.add(tableFromStream.insertInto(sinkDescriptor));

// attach both pipelines to StreamExecutionEnvironment
// (the statement set will be cleared after calling this method)
        statementSet.attachAsDataStream();


// use DataStream API to submit the pipelines
        env.execute();
    }
}

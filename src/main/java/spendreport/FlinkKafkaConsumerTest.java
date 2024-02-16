//package spendreport;
//
//import org.apache.flink.api.common.serialization.DeserializationSchema;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.Schema;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//
//import java.io.IOException;
//import java.util.Properties;
//
//
//public class FlinkKafkaConsumerTest {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env =
//                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//
//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .inStreamingMode()
//                .build();
//
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        properties.setProperty("group.id", "test");
//        // kafka 数据源
//        DataStream<String> r = env.addSource(new FlinkKafkaConsumer<Row>("topic", new SimpleStringSchema<String>(), properties));
//        // 将 DataStream 转为一个 Table API 中的 Table 对象进行使用
//        Table sourceTable = tEnv.fromDataStream(r
//                , Schema
//                        .newBuilder()
//                        .column("f0", "string")
//                        .column("f1", "string")
//                        .column("f2", "bigint")
//                        .columnByExpression("proctime", "PROCTIME()")
//                        .build());
//
//        tEnv.createTemporaryView("source_table", sourceTable);
//
//        String selectWhereSql = "select f0 from source_table where f1 = 'b'";
//
//        Table resultTable = tEnv.sqlQuery(selectWhereSql);
//
//        tEnv.toRetractStream(resultTable, Row.class).print();
//
//        env.execute();
//    }
//}

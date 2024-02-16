package spendreport;

import entity.WC;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class ProcessTimeTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.创建TableEnvironment(Blink planner)
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        // 3.stream数据源
        DataStream<WC> dataStream = env.fromElements(
                new WC("java", 1),
                new WC("spark", 1),
                new WC("hive", 1),
                new WC("hbase", 1),
                new WC("hbase", 1),
                new WC("hadoop", 1),
                new WC("java", 1));

        // 4.将dataStream转换为视图
        // 声明一个额外的字段作为时间属性字段
        tableEnvironment.createTemporaryView("wordcount" ,
                dataStream,
                $("wordName"),
                $("freq"),
                $("user_action_time").proctime());

        // 5.通过SQL对表的查询，生成结果表
        Table table = tableEnvironment.sqlQuery("select * from wordcount");

        // 6.将table表转换为DataStream
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnvironment.toRetractStream(table, Row.class);
        retractStream.print();
        env.execute();
    }
}

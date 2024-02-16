package spendreport;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class EventTimeSQL {
    public static void main(String[] args) throws Exception {
        // 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.创建TableEnvironment(Blink planner)
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        // 3.文件path
        String filePath = "/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/orders.csv";

        // 4.DDL
        // 声明 ts 是事件时间属性，并且用 延迟 3 秒的策略来生成 watermark
        String ddl = "create table orders (\n" +
                " user_id INT,\n" +
                " product STRING,\n" +
                " amount INT,\n" +
                "ts TIMESTAMP(3),\n" +
                "WATERMARK FOR ts AS ts - INTERVAL '3' SECOND \n"  +
                ") WITH (\n" +
                " 'connector.type' = 'filesystem',\n" +
                " 'connector.path' = '"+filePath+"',\n" +
                " 'format.type' = 'csv'\n" +
                ")";

        // 5.创建一个带eventtime字段的表
        tableEnvironment.executeSql(ddl);

        // 6.通过SQL对表的查询，生成结果表
        // 基于事件时间根据滚动窗口统计最近5秒product的数量，amount的总数以及订单数
        String sql = "select TUMBLE_START(ts ,INTERVAL '5' SECOND)," +
                " COUNT(DISTINCT product),\n" +
                " SUM(amount) total_amount,\n" +
                " COUNT(*) order_nums \n" +
                " FROM orders \n" +
                " GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";

        Table table = tableEnvironment.sqlQuery(sql);

        // 7.将table表转换为DataStream
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnvironment.toRetractStream(table, Row.class);
        retractStream.print();
        env.execute();
    }
}

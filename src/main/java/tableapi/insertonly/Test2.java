package tableapi.insertonly;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test2 {

    public static class User {

        public final String name;

        public final Integer score;

        public User(String name, Integer score) {
            this.name = name;
            this.score = score;
        }
    }

    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // create a DataStream
        DataStream<User> dataStream = env.fromElements(
                new User("Alice", 4),
                new User("Bob", 6),
                new User("Alice", 10));

// since fields of a RAW type cannot be accessed, every stream record is treated as an atomic type
// leading to a table with a single column `f0`

        Table table = tableEnv.fromDataStream(dataStream);
        table.printSchema();
// prints:
// (
//  `f0` RAW('User', '...')
// )

// instead, declare a more useful data type for columns using the Table API's type system
// in a custom schema and rename the columns in a following `as` projection

        Table table1 = tableEnv
                .fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .column("f0", DataTypes.of(User.class))
                                .build())
                .as("user");
        table1.printSchema();
// prints:
// (
//  `user` *User<`name` STRING,`score` INT>*
// )

// data types can be extracted reflectively as above or explicitly defined

        Table table2 = tableEnv
                .fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .column(
                                        "f0",
                                        DataTypes.STRUCTURED(
                                                User.class,
                                                DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("score", DataTypes.INT())))
                                .build())
                .as("user");
        table2.printSchema();
    }
}

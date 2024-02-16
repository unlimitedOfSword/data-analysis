package spendreport;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class WordCountTable {
    public static void main(String[] args) {
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);

        // execute a Flink SQL job and print the result locally
        tableEnv.executeSql(
                        // define the aggregation
                        "SELECT word, SUM(frequency) AS `count`\n"
                                // read from an artificial fixed-size table with rows and columns
                                + "FROM (\n"
                                + "  VALUES ('Hello', 1), ('Ciao', 1), ('Hello', 2)\n"
                                + ")\n"
                                // name the table and its columns
                                + "AS WordTable(word, frequency)\n"
                                // group for aggregation
                                + "GROUP BY word")
                .print();
    }
}

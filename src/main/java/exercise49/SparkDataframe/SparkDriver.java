package exercise49.SparkDataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class SparkDriver {

    public static void main(String[] args) {

        String inputPath = args[0], outputPath = args[1];

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 49 - Spark Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> readings = ss
                .read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(inputPath);

        readings.printSchema();
        readings.show();

        ss.udf().register("AgeRange", (Integer age) -> {
            Integer min = (age / 10) * 10;
            Integer max = min + 10;
            return "[" + min + "-" + max + "]";
        }, DataTypes.StringType);

        Dataset<Row> result = readings
                .selectExpr("name", "surname", "AgeRange(age) as range");

        result.printSchema();
        result.show();

        result
                .repartition(1)
                .write()
                .format("csv")
                .option("header", true)
                .save(outputPath);

        ss.stop();

    }
}

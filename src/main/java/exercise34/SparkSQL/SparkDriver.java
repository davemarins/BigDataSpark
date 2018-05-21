package exercise34.SparkSQL;

import exercise34.Readings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0], outputPath = args[1];

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 34 - SparkSQL")
                .master("local")
                .getOrCreate();

        Dataset<Row> readings = ss
                .read()
                .format("csv")
                .option("header", false)
                .option("inferSchema", true)
                .load(inputPath);

        Dataset<Readings> result = readings
                .as(Encoders.bean(Readings.class));

        result.createOrReplaceTempView("readings");

        Dataset<Readings> maxValues = ss
                .sql("SELECT * FROM readings WHERE _c2 = (" +
                        "SELECT MAX(_c2) FROM readings)")
                .as(Encoders.bean(Readings.class));

        maxValues.write().format("csv").save(outputPath);

        ss.close();

    }

}

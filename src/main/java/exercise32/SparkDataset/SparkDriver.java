package exercise32.SparkDataset;

import exercise32.Readings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.max;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0];

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 32 - SparkDataset")
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

        Dataset<Double> maxValue = result
                .agg(max("_c2"))
                .as(Encoders.DOUBLE());

        System.out.println(maxValue.first());

        ss.stop();

    }

}

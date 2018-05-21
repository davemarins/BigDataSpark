package exercise33.SparkSQL;

import exercise33.Readings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0];
        Integer K = Integer.parseInt(args[1]);

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 33 - SparkSQL")
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

        Dataset<Double> maxValue = ss
                .sql("SELECT _c2 FROM readings ORDER BY _c2 DESC")
                .as(Encoders.DOUBLE());

        List<Double> maxValues = maxValue.takeAsList(K);
        for (Double max : maxValues) {
            System.out.println(max);
        }

        ss.stop();

    }

}

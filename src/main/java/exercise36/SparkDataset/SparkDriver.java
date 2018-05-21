package exercise36.SparkDataset;

import exercise36.Readings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0];

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 36 - SparkDataset")
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

        Double maxValue = result
                .agg(avg("_c2"))
                .as(Encoders.DOUBLE())
                .first();

        System.out.println(maxValue.toString());

        ss.close();

    }

}

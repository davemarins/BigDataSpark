package exercise35.SparkDataset;

import exercise35.DateFormat;
import exercise35.Readings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.max;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0], outputPath = args[1];

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 35 - SparkDataset")
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
                .agg(max("_c2"))
                .as(Encoders.DOUBLE())
                .first();

        Dataset<DateFormat> maxReadings = result
                .filter(
                        p -> maxValue.equals(p.get_c2())
                )
                .map(
                        p -> new DateFormat(p.get_c1()), Encoders.bean(DateFormat.class)
                );

        maxReadings.write().format("csv").save(outputPath);

    }

}

package exercise34.SparkDataset;

import exercise34.Readings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.max;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0], outputPath = args[1];

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 34 - SparkDatasetToDo")
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

        Dataset<Readings> maxReadings = result
                .filter(
                        p -> maxValue.equals(p.get_c2())
                );

        maxReadings.write().format("csv").save(outputPath);

    }

}

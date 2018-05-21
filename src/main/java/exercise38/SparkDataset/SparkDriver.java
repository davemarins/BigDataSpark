package exercise38.SparkDataset;

import exercise38.Readings;
import exercise38.Results;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0], outputPath = args[1];

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 38 - SparkDataset")
                .master("local")
                .getOrCreate();

        Dataset<Row> readings = ss
                .read()
                .format("csv")
                .option("header", false)
                .option("inferSchema", true)
                .load(inputPath);

        Dataset<Readings> temp = readings
                .as(Encoders.bean(Readings.class));

        Dataset<Results> result = temp
                .filter(
                        p -> p.get_c2() > 50
                )
                .groupBy("_c0")
                .count()
                .withColumnRenamed("_c0", "sensorId")
                .as(Encoders.bean(Results.class))
                .filter(
                        p -> p.getCount() >= 2
                );

        result.write().format("csv").save(outputPath);

        ss.close();

    }

}

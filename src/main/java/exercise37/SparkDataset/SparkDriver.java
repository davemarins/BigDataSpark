package exercise37.SparkDataset;

import exercise37.Readings;
import exercise37.Results;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0], outputPath = args[1];

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 37 - SparkDataset")
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
                .groupBy("_c0")
                .max("_c2")
                .withColumnRenamed("_c0", "sensorId")
                .withColumnRenamed("max(_c2)", "maxPM10")
                .as(Encoders.bean(Results.class));

        result.write().format("csv").save(outputPath);

        ss.close();

    }

}

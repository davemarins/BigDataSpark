package exercise37.SparkSQL;

import exercise37.Readings;
import exercise37.Results;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

    public static void main(String[] args) {

        String inputPath = args[0];
        String outputPath = args[1];

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 37 - SparkSQL")
                .master("local")
                .getOrCreate();

        Dataset<Row> dfReadings = ss
                .read()
                .format("csv")
                .option("header", false)
                .option("inferSchema", true)
                .load(inputPath);

        Dataset<Readings> dsReadings = dfReadings
                .as(Encoders.bean(Readings.class));

        dsReadings.createOrReplaceTempView("readings");

        Dataset<Results> maxValuePerSensorDS = ss
                .sql("SELECT _c0 as sensorId, max(_c2) as maxPM10 FROM readings GROUP BY _c0")
                .as(Encoders.bean(Results.class));

        maxValuePerSensorDS.write().format("csv").save(outputPath);

        ss.stop();
    }
}

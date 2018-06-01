package exercise38.SparkSQL;

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
                .appName("Exercise 38 - SparkSQL")
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
                .sql("SELECT _c0 as sensorId, count(*) as count FROM readings WHERE _c2 > 50 GROUP BY _c0" +
                        " HAVING COUNT(*) >= 2")
                .as(Encoders.bean(Results.class));

        maxValuePerSensorDS.write().format("csv").save(outputPath);

        ss.stop();

    }

}

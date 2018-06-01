package exercise33.SparkDataset;

import exercise33.PM10;
import exercise33.Readings;
import org.apache.spark.sql.*;

import java.util.List;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0];
        Integer K = Integer.parseInt(args[1]);

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 33 - SparkDatasetToDo")
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

        List<Double> pm10Values = result
                .map(
                        p -> new PM10(p.get_c2()), Encoders.bean(PM10.class)
                ).sort(
                        new Column("pm10").desc()
                ).as(
                        Encoders.DOUBLE()
                ).takeAsList(K);

        for (Double max : pm10Values) {
            System.out.println(max);
        }

    }

}

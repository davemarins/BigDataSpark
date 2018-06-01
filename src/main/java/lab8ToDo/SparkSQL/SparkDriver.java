package lab8ToDo.SparkSQL;

import lab8ToDo.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath1 = args[0], inputPath2 = args[1], outputPath = args[2];
        Double threshold = Double.parseDouble(args[3]);

        SparkSession ss = SparkSession.builder()
                .appName("Lab8 - SparkSQL")
                .master("local")
                .getOrCreate();

        Dataset<Reading> readings = ss
                .read()
                .format("csv")
                .option("delimiter", "\\t")
                .option("header", true)
                .option("inferSchema", true)
                .load(inputPath1)
                .as(Encoders.bean(Reading.class));

        readings.createOrReplaceTempView("readings");

        ss
                .udf()
                .register("weekDay", (Timestamp t) -> Utils.DayOfTheWeek(t), DataTypes.StringType);

        ss
                .udf()
                .register("full", (Integer p) -> {
                    if (p == 0) {
                        return 1;
                    } else {
                        return 0;
                    }
                }, DataTypes.IntegerType);

        ss
                .sql("SELECT station, weekDay(timestamp) as weekDay, hour(timestamp) as hour, "
                        + "avg(full(free_slots)) as critical FROM readings WHERE free_slots <> 0 OR used_slots <> 0 "
                        + "GROUP BY station, weekDay(timestamp), hour(timestamp) " + "HAVING avg(full(free_slots)) > "
                        + threshold)
                .as(Encoders.bean(criticalStation.class))
                .createOrReplaceTempView("criticalStation");

        Dataset<Station> stations = ss
                .read()
                .format("csv")
                .option("delimiter", "\\t")
                .option("header", true)
                .option("inferSchema", true)
                .load(inputPath2)
                .as(Encoders.bean(Station.class));

        stations.createOrReplaceTempView("stations");

        Dataset<Result> result = ss
                .sql("SELECT station, weekDay, hour, longitude, latitude, critical FROM criticalStation, stations "
                        + "WHERE criticalStation.station = stations.id ORDER BY critical DESC, station, weekDay, hour")
                .as(Encoders.bean(Result.class));

        result.show();

        result
                .repartition(1)
                .write()
                .format("csv")
                .option("header", true)
                .save(outputPath);

        ss.close();

    }

}

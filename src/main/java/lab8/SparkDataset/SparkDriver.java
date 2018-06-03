package lab8.SparkDataset;

import lab8.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;

public class SparkDriver {

    public static void main(String[] args) {

        String inputPath1 = args[0], inputPath2 = args[1], outputPath = args[2];
        Double threshold = Double.parseDouble(args[3]);

        SparkSession ss = SparkSession.builder()
                .appName("Lab 8 - SparkDataset")
                .master("local")
                .getOrCreate();

        Dataset<StationDayHourCritical> result1 = ss
                .read()
                .format("csv")
                .option("delimiter", "\\t")
                .option("header", true)
                .option("inferSchema", true)
                .load(inputPath1)
                .as(Encoders.bean(Reading.class))
                .filter(
                        p -> p.getFree_slots() != 0 || p.getUsed_slots() != 0
                )
                .map(
                        p -> new StationDayHourStatus(
                                p.getStation(),
                                Utils.DayOfTheWeek(p.getTimestamp()),
                                Utils.hour(p.getTimestamp()),
                                Utils.full(p.getFree_slots())
                        ), Encoders.bean(StationDayHourStatus.class)
                )
                .groupBy("station", "weekDay", "hour")
                .agg(avg("status"))
                .withColumnRenamed("avg(status)", "critical")
                .as(Encoders.bean(StationDayHourCritical.class))
                .filter(p -> p.getCritical() > threshold);

        Dataset<Station> stations = ss
                .read()
                .format("csv")
                .option("delimiter", "\\t")
                .option("header", true)
                .option("inferSchema", true)
                .load(inputPath2)
                .as(Encoders.bean(Station.class));

        Dataset<Result> result2 = result1
                .join(
                        stations,
                        result1
                                .col("station")
                                .equalTo(stations.col("id")))
                .selectExpr("station", "weekDay", "hour", "longitude", "latitude", "critical")
                .sort(
                        new Column("critical").desc(),
                        new Column("station"),
                        new Column("weekDay"),
                        new Column("hour")
                )
                .as(Encoders.bean(Result.class));

        result2
                .repartition(1)
                .write()
                .format("csv")
                .option("header", true)
                .save(outputPath);

        ss.stop();

    }

}

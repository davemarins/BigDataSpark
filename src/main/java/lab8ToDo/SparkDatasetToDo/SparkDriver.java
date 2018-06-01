package lab8ToDo.SparkDatasetToDo;

import lab8ToDo.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath1 = args[0], inputPath2 = args[1], outputPath = args[2];
        Double threshold = Double.parseDouble(args[3]);

        SparkSession ss = SparkSession.builder()
                .appName("Lab8 - SparkDatasetToDo")
                .master("local")
                .getOrCreate();

        Dataset<criticalStation> readings = ss
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
                        p -> new StationDayHour(p.getStation(),
                                Utils.DayOfTheWeek(p.getTimestamp()),
                                Utils.hour(p.getTimestamp()),
                                Utils.full(p.getFree_slots())), Encoders.bean(StationDayHour.class)
                )
                .groupBy("station", "weekDay", "hour")
                .agg(
                        avg("status")
                )
                .withColumnRenamed(
                        "avg(status)", "critical"
                )
                .as(Encoders.bean(criticalStation.class))
                .filter(
                        p -> p.getCritical() >= threshold
                );

        Dataset<Station> stations = ss
                .read()
                .format("csv")
                .option("delimiter", "\\t")
                .option("header", true)
                .option("inferSchema", true)
                .load(inputPath2)
                .as(Encoders.bean(Station.class));

        Dataset<Result> result = readings
                .join(stations, readings.col("station").equalTo(stations.col("id")))
                .selectExpr("station", "weekDay", "hour", "longitude", "latitude", "critical")
                .sort(new Column[]{
                        new Column("critical").desc(),
                        new Column("station"),
                        new Column("weekDay"),
                        new Column("hour")
                })
                .as(Encoders.bean(Result.class));

        result
                .repartition(1)
                .write()
                .format("csv")
                .option("header", true)
                .save(outputPath);

        ss.close();

    }

}

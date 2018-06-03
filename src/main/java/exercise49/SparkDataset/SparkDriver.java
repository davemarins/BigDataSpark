package exercise49.SparkDataset;

import exercise49.Profile;
import exercise49.Range;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

    public static void main(String[] args) {

        String inputPath = args[0], outputPath = args[1];

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 49 - SparkDataset")
                .master("local")
                .getOrCreate();

        Dataset<Range> result = ss
                .read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(inputPath)
                .as(Encoders.bean(Profile.class))
                .map(
                        p -> {
                            // no Integer, just int, that wouldn't work
                            int min = (p.getAge() / 10) * 10;
                            int max = min + 1;
                            return new Range(p.getName(), p.getSurname(), "[" + min + "-" + max + "]");
                        }, Encoders.bean(Range.class)
                );

        result
                .repartition(1)
                .write()
                .format("csv")
                .option("header", true)
                .save(outputPath);

        ss.stop();

    }
}

package exercise35.SparkDataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.max;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0], outputPath = args[1];

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 35 - SparkDataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> readings = ss
                .read()
                .format("csv")
                .option("header", false)
                .option("inferSchema", true)
                .load(inputPath);

        Double maxValue = readings
                .agg(max("_c2"))
                .first()
                .getAs("max(_c2)");

        Dataset<Row> result = readings
                .filter(
                        "_c2 = " + maxValue
                )
                .select("_c1");

        result.write().format("csv").save(outputPath);

        ss.close();

    }

}

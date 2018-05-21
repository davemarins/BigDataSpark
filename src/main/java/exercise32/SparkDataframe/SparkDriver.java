package exercise32.SparkDataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.max;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0];

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 32 - SparkDataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> readings = ss
                .read()
                .format("csv")
                .option("header", false)
                .option("inferSchema", true)
                .load(inputPath);

        Dataset<Row> maxValue = readings
                .agg(max("_c2"));

        Row rowMaxValue = maxValue.first();

        Double maxValueDouble = rowMaxValue.getAs("max(_c2)");

        System.out.println(maxValueDouble);

        ss.stop();

    }

}

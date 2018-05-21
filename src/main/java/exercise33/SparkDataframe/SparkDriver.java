package exercise33.SparkDataframe;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0];
        Integer K = Integer.parseInt(args[1]);

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 33 - SparkDataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> readings = ss
                .read()
                .format("csv")
                .option("header", false)
                .option("inferSchema", true)
                .load(inputPath);

        List<Row> maxValue = readings
                .select("_c2")
                .sort(new Column("_c2").desc())
                .takeAsList(K);

        for (Row max : maxValue) {
            System.out.println(max.getAs("_c2").toString());
        }

        ss.close();

    }

}

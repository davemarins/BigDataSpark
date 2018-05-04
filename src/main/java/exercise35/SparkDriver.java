package exercise35;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Exercise 35 - Dates associated to max value");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> readFile = sc.textFile(inputPath);

        Double maxValue = readFile
                .map(
                        p -> Double.parseDouble(p.split(",")[2])
                )
                .top(1)
                .get(0);

        JavaRDD<String> rows = readFile
                .filter(
                        p -> p.contains("," + maxValue)
                )
                .map(
                        p -> p.split(",")[1]
                );

        rows.saveAsTextFile(outputPath);
        sc.close();

    }

}

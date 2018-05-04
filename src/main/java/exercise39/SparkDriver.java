package exercise39;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkDriver {

    public static void main(String[] args) {

        String inputPath = args[0], outputPath = args[1];
        Double threshold = Double.parseDouble(args[2]);

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Exercise 39 - Critical dates analysis");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> readFile = sc.textFile(inputPath);

        JavaRDD<String> readingsAboveThreshold = readFile
                .filter(
                        p -> Double.parseDouble(p.split(",")[2]) > threshold
                );

        JavaPairRDD<String, Iterable<String>> sensorsCriticalDates = readingsAboveThreshold
                .mapToPair(
                        p -> {
                            String[] fields = p.split(",");
                            return new Tuple2<>(fields[0], fields[1]);
                        }
                )
                .groupByKey();

        sensorsCriticalDates.saveAsTextFile(outputPath);
        sc.close();

    }

}

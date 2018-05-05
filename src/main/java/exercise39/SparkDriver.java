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

        JavaRDD<String> readings = sc.textFile(inputPath);

        JavaRDD<String> readingsHighValue = readings
                .filter(
                        p -> Double.parseDouble(p.split(",")[2]) > threshold
                );

        JavaPairRDD<String, String> sensorsCriticalDates = readingsHighValue
                .mapToPair(
                        p -> {
                            String[] fields = p.split(",");
                            return new Tuple2<>(fields[0], fields[1]);
                        }
                )
                .reduceByKey(
                        (v1, v2) -> (v1 + ";" + v2)
                );

        sensorsCriticalDates.saveAsTextFile(outputPath);
        sc.close();

    }

}

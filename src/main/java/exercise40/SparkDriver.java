package exercise40;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0], outputPath = args[1];
        Double threshold = Double.parseDouble(args[2]);

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Exercise 40 - Order sensors by number of critical days");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> readings = sc.textFile(inputPath);

        JavaPairRDD<Integer, String> sortedReadingsCriticalCount = readings
                .filter(
                        // sensorID,date,recordedValue
                        p -> Double.parseDouble(p.split(",")[2]) > threshold
                )
                .mapToPair(
                        // JavaPairRDD<String, Integer> (sensorID, 1)
                        p -> new Tuple2<>(p.split(",")[0], 1)
                )
                .reduceByKey(
                        // JavaPairRDD<String, Integer> (sensorID, #times)
                        (v1, v2) -> v1 + v2
                )
                .mapToPair(
                        // JavaPairRDD<Integer, String> (#times, sensorID)
                        p -> new Tuple2<>(p._2(), p._1())
                )
                .sortByKey(false);

        sortedReadingsCriticalCount.saveAsTextFile(outputPath);
        sc.close();

    }

}

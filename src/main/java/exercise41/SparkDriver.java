package exercise41;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0], outputPath = args[1];
        Double threshold = Double.parseDouble(args[2]);
        Integer top = Integer.parseInt(args[3]);

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Exercise 41 - Top K most critical sensors");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> readings = sc.textFile(inputPath);

        List<Tuple2<Integer, String>> sortedReadingsCriticalCount = readings
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
                .sortByKey(false)
                .take(top);

        JavaPairRDD<Integer, String> sortedReadingsCriticalCountRDD = sc
                .parallelizePairs(sortedReadingsCriticalCount);

        sortedReadingsCriticalCountRDD.saveAsTextFile(outputPath);
        sc.close();

    }

}

package exercise38;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;

public class SparkDriver {

    public static void main(String[] args) {

        String inputPath = args[0], outputPath = args[1];
        Double threshold = Double.parseDouble(args[2]);
        Integer thresholdSensors = Integer.parseInt(args[3]);

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Exercise 38 - Pollution analysis");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> readFile = sc.textFile(inputPath);

        // version 1

        /*

        JavaRDD<String> aboveThreshold = readFile
                .filter(
                        p ->  Double.parseDouble(p.split(",")[2]) > threshold
                );

        JavaPairRDD<String, Integer> aboveThresholdSensors = aboveThreshold
                .mapToPair(
                        p ->  new Tuple2<>(p.split(",")[0], 1)
                );

        JavaPairRDD<String, Integer> aboveThresholdSensorsCount = aboveThresholdSensors
                .reduceByKey(
                        (p1, p2) -> p1 + p2
                );

        JavaPairRDD<String, Integer> finalSensors = aboveThresholdSensorsCount
                .filter(
                        (Tuple2<String, Integer> p) -> p._2() >= thresholdSensors
                );

        */

        // version 2

        JavaPairRDD<String, Integer> aboveThreshold = readFile
                .flatMapToPair(
                        p -> {
                            String[] fields = p.split(",");
                            ArrayList<Tuple2<String, Integer>> sensorOne = new ArrayList<>();
                            if (Double.parseDouble(fields[2]) > threshold) {
                                sensorOne.add(new Tuple2<>(fields[0], 1));
                            }
                            return sensorOne.iterator();
                        }
                );

        JavaPairRDD<String, Integer> aboveThresholdCount = aboveThreshold
                .reduceByKey(
                        (p1, p2) -> p1 + p2
                );

        JavaPairRDD<String, Integer> aboveThresholdSensorCount = aboveThresholdCount
                .filter(
                        (Tuple2<String, Integer> p) -> p._2() >= thresholdSensors
                );

        aboveThresholdSensorCount.saveAsTextFile(outputPath);
        sc.close();

    }

}

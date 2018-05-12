package lab6;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0], outputPath = args[1];

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Lab 6");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> readings = sc.textFile(inputPath);

        JavaPairRDD<Integer, String> finalResult = readings
                .filter(
                        // skip first line
                        p -> !p.startsWith("Id,ProductId,UserId")
                )
                .mapToPair(
                        // (userID, productID)
                        p -> {
                            String[] fields = p.split(",");
                            return new Tuple2<>(fields[2], fields[1]);
                        }
                )
                .groupByKey()
                .values()
                .flatMapToPair(
                        // (userID;productID, 1)
                        p -> {
                            List<Tuple2<String, Integer>> tempResult = new ArrayList<>();
                            for (String p1 : p) {
                                for (String p2 : p) {
                                    if (p1.compareTo(p2) > 0) {
                                        tempResult.add(new Tuple2<>(p1 + ";" + p2, 1));
                                    }
                                }
                            }
                            return tempResult.iterator();
                        }
                )
                .reduceByKey(
                        // (userID;productID, #times)
                        (v1, v2) -> v1 + v2
                )
                .filter(
                        // #times > 1
                        p -> p._2 > 1
                )
                .mapToPair(
                        // (#times, userID;productID)
                        p -> new Tuple2<>(p._2, p._1)
                )
                .sortByKey(false);

        finalResult.saveAsTextFile(outputPath);
        sc.close();

    }

}

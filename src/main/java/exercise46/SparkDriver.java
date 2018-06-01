package exercise46;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0], outputPath = args[1];

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Exercise 46 - Time series analysis");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Iterable<Temperature>> result = sc
                .textFile(inputPath)
                .flatMapToPair(
                        p -> {
                            // timestamp,temperature
                            String[] readings = p.split(",");
                            List<Tuple2<Integer, Temperature>> tempResult = new ArrayList<>();
                            Tuple2<Integer, Temperature> pair;
                            for (Integer i = 0; i <= 120; i += 60) {
                                pair = new Tuple2<>(
                                        Integer.parseInt(readings[0]) - i,
                                        new Temperature(
                                                Integer.parseInt(readings[0]), Double.parseDouble(readings[1])
                                        )
                                );
                                tempResult.add(pair);
                            }
                            return tempResult.iterator();
                        }
                ) // JavaPairRDD<Integer, TimeStampTemperature>
                .groupByKey() // JavaPairRDD<Integer, Iterable<Temperature>>
                .values() // JavaRDD<Iterable<Temperature>>
                .filter(
                        p -> {
                            HashMap<Integer, Double> temp = new HashMap<>();
                            Integer minTimestamp = Integer.MAX_VALUE;
                            for (Temperature t : p) {
                                temp.put(t.getTimestamp(), t.getTemperature());
                                if (t.getTimestamp() < minTimestamp) {
                                    minTimestamp = t.getTimestamp();
                                }
                            }
                            if (temp.size() == 3) {
                                for (Integer ts = minTimestamp + 60; ts <= minTimestamp + 120; ts += 60) {
                                    if (temp.get(ts) <= temp.get(ts - 60)) {
                                        return false;
                                    }
                                }
                                return true;
                            } else {
                                return false;
                            }
                        }
                );

        result.saveAsTextFile(outputPath);

        sc.close();

    }

}

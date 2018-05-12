package exercise43;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath1 = args[0], inputPath2 = args[1];
        String outputPath1 = args[2], outputPath2 = args[3], outputPath3 = args[4];
        Integer threshold = Integer.parseInt(args[5]);

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Exercise 43 - Critical bike sharing station" +
                " analysis");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 1st part

        JavaRDD<String> readings = sc.textFile(inputPath1).cache();

        JavaPairRDD<Double, String> finalResult1 = readings
                .mapToPair(
                        p -> {
                            String[] fields = p.split(",");
                            // free slots < threshold
                            if (Integer.parseInt(fields[5]) < threshold) {
                                return new Tuple2<>(fields[0], new Readings(1.0, 1.0));
                            } else {
                                return new Tuple2<>(fields[0], new Readings(1.0, 0.0));
                            }
                        }
                )
                .reduceByKey(
                        (v1, v2) -> new Readings(
                                v1.totalReadings + v2.totalReadings,
                                v1.criticalReadings + v2.criticalReadings)
                )
                .mapValues(
                        p -> p.criticalReadings / p.totalReadings
                )
                .filter(
                        p -> p._2 > 0.8
                )
                .mapToPair(
                        p -> new Tuple2<>(p._2, p._1)
                )
                .sortByKey(false);

        finalResult1.saveAsTextFile(outputPath1);

        // 2nd part

        JavaPairRDD<Double, String> finalResult2 = readings
                .mapToPair(
                        p -> {
                            String[] fields = p.split(",");
                            // time slots creation
                            int minTimeSlotHour = 4 * (Integer.parseInt(fields[2]) / 4);
                            int maxTimeSlotHour = minTimeSlotHour + 3;
                            String timestamp = "ts[" + minTimeSlotHour + "-" + maxTimeSlotHour + "]";
                            String key = timestamp + "-" + fields[0];
                            if (Integer.parseInt(fields[5]) < threshold) {
                                return new Tuple2<>(key, new Readings(1.0, 1.0));
                            } else {
                                return new Tuple2<>(key, new Readings(1.0, 0.0));
                            }
                        }
                )
                .reduceByKey(
                        (v1, v2) -> new Readings(
                                v1.totalReadings + v2.totalReadings,
                                v1.criticalReadings + v2.criticalReadings)
                )
                .mapValues(
                        p -> p.criticalReadings / p.totalReadings
                )
                .filter(
                        p -> p._2 > 0.8
                )
                .mapToPair(
                        p -> new Tuple2<>(p._2, p._1)
                )
                .sortByKey(false);

        finalResult2.saveAsTextFile(outputPath2);

        // 3rd part

        JavaRDD<String> neighborsReadings = sc.textFile(inputPath2);

        JavaPairRDD<String, List<String>> tempResult3 = neighborsReadings
                .mapToPair(
                        p -> {
                            String[] fields = p.split(",");
                            return new Tuple2<>(fields[0],
                                    new ArrayList<>(Arrays.asList(fields[1].split(" "))));
                        }
                );

        HashMap<String, List<String>> neighbors = new HashMap<>();
        for (Tuple2<String, List<String>> pair : tempResult3.collect()) {
            neighbors.put(pair._1(), pair._2());
        }

        final Broadcast<HashMap<String, List<String>>> neighborsBroadcast = sc.broadcast(neighbors);

        JavaRDD<String> finalResult3 = readings
                .filter(
                        p -> Integer.parseInt(p.split(",")[5]) == 0
                ).mapToPair(
                        p -> {
                            String[] fields = p.split(",");
                            return new Tuple2<>(fields[1] + fields[2] + fields[3], p);
                        }
                )
                .groupByKey()
                .flatMap(
                        (Tuple2<String, Iterable<String>> p) -> {

                            List<String> selectedReading = new ArrayList<>();
                            List<String> stations = new ArrayList<>();
                            for (String reading : p._2) {
                                stations.add(reading.split(",")[0]);
                            }

                            for (String reading : p._2) {

                                String[] fields = reading.split(",");
                                List<String> numberCurrentStation = neighborsBroadcast.value().get(fields[0]);
                                boolean allNeighborsFull = true;
                                for (String neighborStation : numberCurrentStation) {
                                    if (!stations.contains(neighborStation)) {
                                        allNeighborsFull = false;
                                    }
                                }
                                if (allNeighborsFull) {
                                    selectedReading.add(reading);
                                }

                            }

                            return selectedReading.iterator();

                        }
                );

        finalResult3.saveAsTextFile(outputPath3);

        sc.close();
    }

    public static class Readings implements Serializable {

        private Double totalReadings, criticalReadings;

        Readings(Double totalReadings, Double criticalReadings) {
            this.totalReadings = totalReadings;
            this.criticalReadings = criticalReadings;
        }

    }

}

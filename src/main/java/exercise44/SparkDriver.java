package exercise44;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath1 = args[0], inputPath2 = args[1], inputPath3 = args[2], outputPath = args[3];
        Double threshold = Double.parseDouble(args[4]);

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Exercise 44 - Misleading profile selection");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, String> watchedRDD = sc
                .textFile(inputPath1)
                .mapToPair(
                        p -> {
                            String[] fields = p.split(",");
                            // movieId,userId
                            return new Tuple2<>(fields[1], fields[0]);
                        }
                );

        JavaPairRDD<String, String> watchedGenreRDD = sc
                .textFile(inputPath3)
                .mapToPair(
                        p -> {
                            String[] fields = p.split(",");
                            // movieId,movieGenre
                            return new Tuple2<>(fields[0], fields[2]);
                        }
                )
                // movieId,userId,movieGenre
                .join(watchedRDD)
                .mapToPair(
                        // userId,movieGenre
                        p -> new Tuple2<>(p._2._2, p._2._1)
                );

        JavaPairRDD<String, String> preferenceRDD = sc
                .textFile(inputPath2)
                .mapToPair(
                        p -> {
                            String[] fields = p.split(",");
                            // userId,movieGenre
                            return new Tuple2<>(fields[0], fields[1]);
                        }
                );

        JavaRDD<String> result = watchedGenreRDD
                .cogroup(preferenceRDD)
                .filter(
                        p -> {
                            List<String> liked = new ArrayList<>();
                            for (String like : p._2._2) {
                                liked.add(like);
                            }
                            Integer count = 0, dislike = 0;
                            for (String genre : p._2._1) {
                                if (!liked.contains(genre)) {
                                    dislike++;
                                }
                                count++;
                            }
                            return dislike > (threshold * count);
                        }
                )
                .keys();

        result.saveAsTextFile(outputPath);

        sc.close();

    }

}

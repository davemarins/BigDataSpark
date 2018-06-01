package exercise45;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath1 = args[0], inputPath2 = args[1], inputPath3 = args[2], outputPath = args[3];
        Double threshold = Double.parseDouble(args[4]);

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Exercise 45 - Profile update");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, String> watchedGenreRDD = sc
                .textFile(inputPath3)
                .mapToPair(
                        p -> {
                            String[] fields = p.split(",");
                            // movieId,movieGenre
                            return new Tuple2<>(fields[0], fields[2]);
                        }
                );

        JavaPairRDD<String, String> userGenreRDD = sc
                .textFile(inputPath1)
                .mapToPair(
                        p -> {
                            String[] fields = p.split(",");
                            // movieId,userId
                            return new Tuple2<>(fields[1], fields[0]);
                        }
                )
                .join(watchedGenreRDD)
                .mapToPair(
                        // userId,movieGenre
                        p -> new Tuple2<>(p._2._1, p._2._2)
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

        JavaPairRDD<String, String> result = userGenreRDD
                .cogroup(preferenceRDD)
                .filter(
                        p -> {
                            List<String> liked = new ArrayList<>();
                            Integer count = 0, dislike = 0;
                            for (String like : p._2()._2()) {
                                liked.add(like);
                            }
                            for (String genre : p._2()._1()) {
                                if (!liked.contains(genre)) {
                                    dislike++;
                                }
                                count++;
                            }
                            return dislike > (threshold * count);
                        }
                )
                .flatMapValues(
                        p -> {
                            List<String> selectedGenres = new ArrayList<>();
                            List<String> likedGenres = new ArrayList<>();
                            HashMap<String, Integer> countGenre = new HashMap<>();
                            for (String like : p._2) {
                                likedGenres.add(like);
                            }
                            for (String watched : p._1) {
                                if (!likedGenres.contains(watched)) {
                                    Integer count = countGenre.get(watched);
                                    if (count == null) {
                                        countGenre.put(watched, 1);
                                    } else {
                                        countGenre.put(watched, count + 1);
                                    }
                                }
                            }
                            for (String genre : countGenre.keySet()) {
                                if (countGenre.get(genre) >= 5) {
                                    selectedGenres.add(genre);
                                }
                            }
                            return selectedGenres;
                        }
                );

        result.saveAsTextFile(outputPath);

        sc.close();

    }

}

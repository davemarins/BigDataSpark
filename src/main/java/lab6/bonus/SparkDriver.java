package lab6.bonus;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0];

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Lab 6");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> readings = sc.textFile(inputPath);

        List<Tuple2<String, Integer>> finalResult = readings
                .filter(
                        p -> !p.startsWith("Id,ProductId,UserId")
                )
                .mapToPair(
                        p -> {
                            String[] fields = p.split(",");
                            return new Tuple2<>(fields[2], fields[1]);
                        }
                )
                .groupByKey()
                .values()
                .flatMapToPair(
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
                        (p1, p2) -> p1 + p2
                )
                .filter(
                        p -> p._2 > 1
                )
                .top(
                        10, new MyComparatorBigData()
                );

        for (Tuple2<String, Integer> result : finalResult) {
            System.out.println(result);
        }

        sc.close();
    }

    public static class MyComparatorBigData implements Comparator<Tuple2<String, Integer>>, Serializable {

        @Override
        public int compare(Tuple2<String, Integer> p1, Tuple2<String, Integer> p2) {
            return p1._2.compareTo(p2._2);
        }

    }

}


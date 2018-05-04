package exercise37;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Exercise 37 - Maximum values");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> readFile = sc.textFile(inputPath);

        JavaPairRDD<String, Double> values = readFile
                .mapToPair(
                        p -> {
                            String[] fields = p.split(",");
                            return new Tuple2<>(fields[0], Double.parseDouble(fields[2]));
                        }
                );

        JavaPairRDD<String, Double> maxValues = values
                .reduceByKey(
                        (v1, v2) -> v1.compareTo(v2) > 0 ? v1 : v2
                );

        maxValues.saveAsTextFile(outputPath);

        sc.close();

    }

}

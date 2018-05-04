package exercise36;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0];

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Exercise 36 - Average value");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> readFile = sc.textFile(inputPath);

        JavaRDD<Double> values = readFile
                .map(
                        p -> Double.parseDouble(p.split(",")[2])
                );

        Double accumulator = values
                .reduce(
                        (v1, v2) -> v1 + v2
                );
        Long counter = values.count();

        System.out.println("Average value => " + accumulator / counter);
        sc.close();

    }

}

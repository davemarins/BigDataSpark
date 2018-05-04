package exercise32;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0];

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Exercise 32 - Maximum value");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> readFile = sc.textFile(inputPath);

        Double maxValue1 = readFile
                .map(
                        p -> {
                            String[] fields = p.split(",");
                            return Double.parseDouble(fields[2]);
                        }
                )
                .reduce(
                        (v1, v2) -> {
                            if (v1 > v2)
                                return v1;
                            else
                                return v2;
                        }
                );

        List<Double> maxValue2 = readFile
                .map(
                        p -> Double.parseDouble(p.split(",")[2])
                )
                .top(1);

        System.out.println("Max value from .map().reduce() => " + maxValue1);
        // .get(0) is useless, but that's good programming
        System.out.println("Max value from .map().top(1) => " + maxValue2.get(0));

        sc.close();

    }

}

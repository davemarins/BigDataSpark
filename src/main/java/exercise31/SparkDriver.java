package exercise31;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

    public static void main(String[] args) {

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Exercise 31 - Log analysis");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logRDD = sc.textFile(inputPath).filter(logLine -> logLine
                .toLowerCase()
                .contains("www.google.com"))
                .map(p -> p.split("\\s+")[0])
                .distinct();
        logRDD.saveAsTextFile(outputPath);
        sc.close();

    }

}

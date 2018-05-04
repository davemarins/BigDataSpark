package exercise30;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

    public static void main(String[] args) {

        String inputPath = args[0];
        String outputPath = args[1];

        //  .setMaster("local") only for local testing, remove it before running on a cluster

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Exercise 30 - Log filtering");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logRDD = sc.textFile(inputPath);
        JavaRDD<String> googleRDD = logRDD.filter(logLine -> logLine.toLowerCase().contains("google"));
        googleRDD.saveAsTextFile(outputPath);
        sc.close();

    }

}

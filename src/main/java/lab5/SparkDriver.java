package lab5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0], outputPath = args[1], starting = args[2];

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Lab 5");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> readings = sc.textFile(inputPath);

        JavaRDD<String> result = readings
                .filter(
                        p -> p.split("\t")[0].toLowerCase().startsWith(starting.toLowerCase())
                );

        result.saveAsTextFile(outputPath);

        sc.close();

    }


}

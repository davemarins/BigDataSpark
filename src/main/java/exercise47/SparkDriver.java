package exercise47;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class SparkDriver {

    public static void main(String args[]) throws InterruptedException {

        String inputPath = args[0], outputPath = args[1];

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Exercise 47 - Full station id in real time");

        JavaStreamingContext strc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaSparkContext spc = strc.sparkContext();

        JavaPairRDD<String, String> stations = spc
                .textFile(inputPath)
                .mapToPair(
                        p -> {
                            String[] fields = p.split("\t");
                            return new Tuple2<>(fields[0], fields[3]);
                        }
                );

        JavaReceiverInputDStream<String> readings = strc.socketTextStream("127.0.0.1", 9999);

        final JavaDStream<Tuple2<String, String>> result = readings
                .filter(
                        p -> {
                            String[] fields = p.split(",");
                            return Integer.parseInt(fields[1]) == 0;
                        }
                ).mapToPair(
                        p -> {
                            String[] fields = p.split(",");
                            return new Tuple2<>(fields[0], fields[3]);
                        }
                ).transform(
                        p -> p.join(stations).values()
                );

        result.print();

        result.dstream().saveAsTextFiles(outputPath, "");

        strc.start();

        strc.awaitTerminationOrTimeout(60000);

        strc.close();

    }

}

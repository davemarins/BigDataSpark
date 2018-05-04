package exercise33;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

// import org.apache.spark.api.java.function.Function;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0];
        Integer K = Integer.parseInt(args[1]);

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Exercise 33 - Top K maximum values");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> readFile = sc.textFile(inputPath);
        JavaRDD<Double> values = readFile.map(
                p -> {
                    String[] fields = p.split(",");
                    return Double.parseDouble(fields[2]);
                }
        );

        List<Double> finalValues = values.top(K);

        /*

        useless, it's already sorted

        JavaRDD<Double> tempValues = values
                .sortBy(new Function<Double, Double>() {
                    @Override
                    public Double call(Double d) {
                        return d;
                    }
                }, false, 1);

        List<Double> finalValues = tempValues.top(K);

        */

        for (Double finalValue : finalValues) {
            System.out.println("Max value: " + finalValue);
        }
        sc.close();

    }

}

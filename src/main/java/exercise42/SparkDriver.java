package exercise42;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPathQuestions = args[0], inputPathAnswers = args[1], outputPath = args[2];

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Exercise 42 - Mapping question-answer(s)");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> readingQuestions = sc.textFile(inputPathQuestions);
        JavaRDD<String> readingAnswers = sc.textFile(inputPathAnswers);

        // questionID,timestamp,textOfTheQuestion
        // answerID,questionID,timestamp,textOfTheAnswer

        JavaPairRDD<String, String> questionPair = readingQuestions
                .mapToPair(
                        p -> new Tuple2<>(p.split(",")[0], p.split(",")[2])
                );

        JavaPairRDD<String, String> answerPair = readingAnswers
                .mapToPair(
                        p -> new Tuple2<>(p.split(",")[1], p.split(",")[3])
                );

        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> questionsAndAnswers = questionPair.cogroup(answerPair);

        questionsAndAnswers.saveAsTextFile(outputPath);

        // questionID,textOfTheQuestion,List<textOfTheAnswer>

        sc.close();
    }

}

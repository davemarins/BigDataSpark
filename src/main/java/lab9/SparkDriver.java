package lab9;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

    private static boolean testAllUpperCase(String str) {
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c >= 97 && c <= 122) {
                return false;
            }
        }
        return true;
    }

    public static void main(String args[]) {

        SparkSession ss = SparkSession.builder()
                .appName("Lab 9 - Decision Tree and Logistic Regression")
                .master("local")
                .getOrCreate();

        JavaSparkContext jss = new JavaSparkContext(ss.sparkContext());

        // Id, ProductId, UserId, ProfileName
        // HelpfulnessNumerator, HelpfulnessDenominator
        // Score, Time, Summary, Text

        // instead of the simpler row.split(","); this will
        // ignore the commas followed by an odd number of quotes
        // row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        JavaRDD<LabeledPoint> result = jss
                .textFile(args[0])
                .filter(new Filter())
                .map(
                        p -> {
                            String[] fields = p.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                            Double classLabel = (Double.parseDouble(fields[4])
                                    / Double.parseDouble(fields[5])) > 0.9 ? 1.0 : 0.0;

                            double[] values = new double[9];

                            // Now I'm choosing some criteria for classification
                            // I consider the Score, the Summary and the Text (6, 8 and 9)
                            // 9 attributes are fine, choose whatever you want

                            // the length of the Summary
                            values[0] = fields[8].length();
                            // the length of the Text
                            values[1] = fields[9].length();
                            // number of words in Summary
                            values[2] = fields[8].split("\\s+").length;
                            // number of words in Text
                            values[3] = fields[9].split("\\s+").length;
                            // number of ! in Summary
                            values[4] = fields[8].split("!").length;
                            // number of ! in Text
                            values[5] = fields[9].split("!").length;

                            int count = 0;
                            for (String text : fields[8].split("\\s+")) {
                                if (testAllUpperCase(text)) {
                                    count++;
                                }
                            }
                            // number of all uppercase words in Summary
                            values[6] = count;
                            count = 0;
                            for (String text : fields[9].split("\\s+")) {
                                if (testAllUpperCase(text)) {
                                    count++;
                                }
                            }
                            // number of all uppercase words in Text
                            values[7] = count;
                            // Score
                            values[8] = Double.parseDouble(fields[6]);

                            Vector attributeValues = Vectors.dense(values);
                            return new LabeledPoint(classLabel, attributeValues);
                        }
                );

        Dataset<Row> reviews = ss
                .createDataFrame(result, LabeledPoint.class)
                .cache();

        reviews.show(5);

        // 30% testing 70% training
        Dataset<Row>[] splits = reviews.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        DecisionTreeClassifier dt = new DecisionTreeClassifier();
        LogisticRegression lr = new LogisticRegression();

        /*
         * new Tokenizer().setInputCol("").setOutputCol("")
         * new StopWordsRemover().setInputCol("").setOutputCol("")
         * new HashingTF().setNumFeatures(number).setInputCol("").setOutputCol("")
         * new IDF().setInputCol("").setOutputCol("");
         */

        Pipeline pipeline1 = new Pipeline()
                .setStages(new PipelineStage[]{dt});

        Dataset<Row> predictions = pipeline1
                .fit(trainingData)
                .transform(testData);
        predictions.show(5);

        MulticlassMetrics metrics = new MulticlassMetrics(predictions.select("prediction", "label"));
        System.out.println("Metrics for DecisionTree");
        System.out.println("Confusion matrix:\n" + metrics.confusionMatrix());
        System.out.println("Accuracy => " + metrics.accuracy());

        Pipeline pipeline2 = new Pipeline()
                .setStages(new PipelineStage[]{lr});

        predictions = pipeline2
                .fit(trainingData)
                .transform(testData);
        predictions.show(5);

        System.out.println("Metrics for LogisticRegression");
        System.out.println("Confusion matrix:\n" + metrics.confusionMatrix());
        System.out.println("Accuracy => " + metrics.accuracy());

        jss.close();

    }

}

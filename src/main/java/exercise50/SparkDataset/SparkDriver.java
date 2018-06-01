package exercise50.SparkDataset;

import exercise50.Profile;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0], outputPath = args[1];

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 49 - SparkDatasetToDo")
                .master("local")
                .getOrCreate();

        ss
                .read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(inputPath)
                .as(Encoders.bean(Profile.class))
                .map(
                        p -> p.getName() + "" + p.getSurname(), Encoders.STRING()
                )
                .repartition(1)
                .write()
                .format("csv")
                .option("header", true)
                .save(outputPath);

        ss.close();

    }

}

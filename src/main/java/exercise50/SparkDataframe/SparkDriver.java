package exercise50.SparkDataframe;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0], outputPath = args[1];
        SparkSession ss = SparkSession.builder()
                .appName("Exercise 50 - SparkDataframe")
                .master("local")
                .getOrCreate();

        ss
                .udf()
                .register("Concat", (String name, String surname) -> name + "" + surname, DataTypes.StringType);

        ss
                .read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(inputPath)
                .selectExpr("Concat(name, surname) as FullName")
                .repartition(1)
                .write()
                .format("csv")
                .option("header", true)
                .save(outputPath);

        ss.close();

    }

}

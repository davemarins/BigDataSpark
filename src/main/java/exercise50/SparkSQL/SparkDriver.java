package exercise50.SparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath = args[0], outputPath = args[1];

        SparkSession ss = SparkSession.builder()
                .appName("Exercise 49 - SparkSQL")
                .master("local")
                .getOrCreate();

        Dataset<Row> readings = ss
                .read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(inputPath);

        readings
                .createOrReplaceTempView("profiles");

        ss
                .udf()
                .register("Concat", (String name, String surname) -> name + "" + surname, DataTypes.StringType);

        Dataset<Row> result = ss
                .sql("SELECT Concat(name, surname) as FullName FROM profiles");

        result
                .repartition(1)
                .write()
                .format("csv")
                .option("header", true)
                .save(outputPath);

        ss.close();

    }

}

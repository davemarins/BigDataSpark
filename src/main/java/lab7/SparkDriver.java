package lab7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class SparkDriver {

    public static void main(String args[]) {

        String inputPath1 = args[0], inputPath2 = args[1], outputFile = args[2];
        Double threshold = Double.parseDouble(args[3]);

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Lab 6");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> readings1 = sc.textFile(inputPath1);
        JavaRDD<String> readings2 = sc.textFile(inputPath2);

        JavaPairRDD<String, CriticalDay> result1 = readings1
                .filter(
                        p -> {
                            if (p.startsWith("s")) {
                                return false;
                            } else {
                                String[] fields = p.split("\\t");
                                return Integer.parseInt(fields[2]) != 0
                                        || Integer.parseInt(fields[3]) != 0;
                            }
                        }
                )
                .mapToPair(
                        p -> {
                            String[] fields = p.split("\\t");
                            String[] timestamp = fields[1].split(" ");
                            CountReadings countRF;
                            if (Integer.parseInt(fields[3]) == 0) {
                                countRF = new CountReadings(1.0, 1.0);
                            } else {
                                countRF = new CountReadings(1.0, 0.0);
                            }
                            return new Tuple2<>(fields[0] + "_" + DateTool.DayOfTheWeek(timestamp[0]) + "_" +
                                    timestamp[1].replaceAll(":.*", ""), countRF);
                        }
                )
                .reduceByKey(
                        (v1, v2) -> new CountReadings(
                                v1.numReadings + v2.numReadings,
                                v1.numTotReadings + v2.numTotReadings
                        )
                ).mapValues(
                        p -> p.numTotReadings / p.numReadings
                ).filter(
                        p -> p._2 >= threshold
                )
                .mapToPair(
                        p -> {
                            String[] fields = p._1.split("_");
                            return new Tuple2<>(fields[0],
                                    new CriticalDay(fields[1], Integer.parseInt(fields[2]), p._2));
                        }
                ).reduceByKey(
                        (v1, v2) -> {
                            if (v1.criticalities > v2.criticalities
                                    || (v1.criticalities.equals(v2.criticalities) && v1.hour < v2.hour)
                                    || (v1.criticalities.equals(v2.criticalities) && v1.hour.equals(v2.hour)
                                    && v1.dayOfWeek.compareTo(v2.dayOfWeek) < 0)
                                    ) {
                                return new CriticalDay(v1.dayOfWeek, v1.hour, v1.criticalities);
                            } else {
                                return new CriticalDay(v2.dayOfWeek, v2.hour, v2.criticalities);
                            }
                        }
                );

        JavaPairRDD<String, String> locationOfStations = readings2
                .mapToPair(
                        p -> {
                            String[] fields = p.split("\\t");
                            return new Tuple2<>(fields[0], fields[1] + "," + fields[2]);
                        }
                );

        JavaRDD<String> result2 = result1
                .join(locationOfStations)
                .map(
                        p -> {
                            String stationId = p._1();
                            CriticalDay day = p._2()._1();
                            String coords = p._2()._2();
                            return "<Placemark><name>" + stationId + "</name>" + "<ExtendedData>"
                                    + "<Data name=\"DayWeek\"><value>" + day.dayOfWeek + "</value></Data>"
                                    + "<Data name=\"Hour\"><value>" + day.hour + "</value></Data>"
                                    + "<Data name=\"Criticality\"><value>" + day.criticalities + "</value></Data>"
                                    + "</ExtendedData>" + "<Point>" + "<coordinates>" + coords + "</coordinates>"
                                    + "</Point>" + "</Placemark>";
                        }
                );

        List<String> localKML = result2.collect();
        Configuration configHadoop = new Configuration();

        try {

            URI uri = URI.create(outputFile);
            FileSystem file = FileSystem.get(uri, configHadoop);
            FSDataOutputStream output = file.create(new Path(uri));
            BufferedWriter bufferedOutput = new BufferedWriter(new OutputStreamWriter(output, "UTF-8"));
            bufferedOutput.write("<kml xmlns=\"http://www.opengis.net/kml/2.2\"><Document>");
            bufferedOutput.newLine();
            for (String lineKML : localKML) {
                bufferedOutput.write(lineKML);
                bufferedOutput.newLine();
            }
            bufferedOutput.write("</Document></kml>");
            bufferedOutput.newLine();
            bufferedOutput.close();
            output.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        sc.close();

    }

    public static class CountReadings implements Serializable {

        private Double numReadings;
        private Double numTotReadings;

        CountReadings(Double numReadings, Double numTotReadings) {
            this.numReadings = numReadings;
            this.numTotReadings = numTotReadings;
        }

    }

    public static class DateTool {

        static String DayOfTheWeek(String date) {

            try {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd");
                Date d = format.parse(date);
                Calendar cal = Calendar.getInstance();
                cal.setTime(d);
                switch (cal.get(Calendar.DAY_OF_WEEK)) {
                    case Calendar.MONDAY:
                        return "Mon";
                    case Calendar.TUESDAY:
                        return "Tue";
                    case Calendar.WEDNESDAY:
                        return "Wed";
                    case Calendar.THURSDAY:
                        return "Thu";
                    case Calendar.FRIDAY:
                        return "Fri";
                    case Calendar.SATURDAY:
                        return "Sat";
                    case Calendar.SUNDAY:
                        return "Sun";
                    default:
                        return "Sat";
                }
            } catch (ParseException e) {
                e.printStackTrace();
                return "";
            }

        }

    }

    public static class CriticalDay implements Serializable {

        private String dayOfWeek;
        private Integer hour;
        private Double criticalities;

        CriticalDay(String dayOfWeek, Integer hour, Double criticalities) {
            this.dayOfWeek = dayOfWeek;
            this.hour = hour;
            this.criticalities = criticalities;
        }

    }


}

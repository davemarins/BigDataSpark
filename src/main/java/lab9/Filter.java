package lab9;

import org.apache.spark.api.java.function.Function;

public class Filter implements Function<String, Boolean> {

    @Override
    public Boolean call(String row) {

        String[] fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        if (row.startsWith("Id")) {
            return false;
        } else {
            return Integer.parseInt(fields[5]) != 0;
        }

    }

}

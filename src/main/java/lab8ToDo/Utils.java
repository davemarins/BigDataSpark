package lab8ToDo;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Utils {

    private static String result(Calendar cal) {
        switch (cal.get(Calendar.DAY_OF_WEEK)) {
            case Calendar.SUNDAY:
                return "Sun";
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
            default:
                return "Mon";
        }
    }

    public static String DayOfTheWeek(Timestamp date) {

        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(date.getTime()));
        return Utils.result(cal);

    }

    public static String DayOfTheWeek(String date) {

        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd");
            Calendar cal = Calendar.getInstance();
            Date d = format.parse(date);
            cal.setTime(d);
            return Utils.result(cal);
        } catch (ParseException e) {
            e.printStackTrace();
            return "";
        }

    }

    public static int hour(Timestamp date) {

        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(date.getTime()));
        return cal.get(Calendar.HOUR_OF_DAY);

    }

    public static int full(int p) {
        if (p == 0) {
            return 1;
        } else {
            return 0;
        }
    }

}

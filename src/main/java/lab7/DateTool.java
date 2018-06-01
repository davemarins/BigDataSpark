package lab7;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTool {

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

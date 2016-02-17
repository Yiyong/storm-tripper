package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by yiguo on 1/19/16.
 */
public class Utils {

    private static Logger logger = LoggerFactory.getLogger(Utils.class);

    private static String dateFormat = "yyyy-MM-dd-HH";

    public static long getCurrentTimeStamp() {
        long unixTime = System.currentTimeMillis() / 1000L;
        return unixTime;
    }

    public static String getDate(long timestamp) {
        Date date = new Date(timestamp * 1000L);
        return new SimpleDateFormat(dateFormat).format(date);
    }

    public static boolean isValidDateInHour(String dateInHour) {
        try {
            new SimpleDateFormat(dateFormat).parse(dateInHour);
        } catch (ParseException e) {
            logger.warn("Not recognized field from Couchbase: " + dateInHour);
            return false;
        }
        return true;
    }

}

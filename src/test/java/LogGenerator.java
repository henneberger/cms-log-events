import com.google.common.collect.ImmutableList;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by henneberger on 8/20/17.
 */
public class LogGenerator {
    public static DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");

    private static final ImmutableList<String> FILE_EVENTS =
            ImmutableList.of(
                    "\"GET /images/opf-logo.gif HTTP/1.0\" 200 32511",
                    "\"GET /images/ksclogosmall.gif HTTP/1.0\" 200 3635",
                    "\"GET /images/ksclogosmall.gif HTTP/1.0\" 403 298",
                    "\"GET /images/example.variable.gif HTTP/1.0\" 200 5512",
                    "\"GET /images/example.variable.gif HTTP/1.0\" 200 4125",
                    "\"GET /home.html HTTP/1.0\" 200 5326",
                    "\"POST /postFile HTTP/1.0\" 201 3255125",
                    "\"GET /live-lb HTTP/1.0\" 200 10", //small values can have a larger variance in count-min-sketch
                    "\"GET /live-lb HTTP/1.0\" 404 10"
            );

    /**
     * Generates log events with the option of creating high cardinality 'random' events
     *
     *  [01/Aug/1995:00:54:59 -0400] "GET /images/opf-logo.gif HTTP/1.0" 200 32511
     */
    public static List<String> randomEvents(int numEvents, long seed, double highCardinalityProbability) {
        List<String> log = new ArrayList<>();

        Random random = new Random(seed);

        ZonedDateTime dateTime = ZonedDateTime.of(2017, 8, 20, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles"));

        for (int i = 0; i < numEvents; i++) {
            ZonedDateTime date = dateTime.plus(i, ChronoUnit.SECONDS);

            String dateStr = DATE_FORMAT.format(date);
            if (random.nextInt(100) / 100d >= highCardinalityProbability) {
                String event = FILE_EVENTS.get(random.nextInt(FILE_EVENTS.size()));
                log.add("[" + dateStr + "] " + event);
            } else {
                String method = random.nextBoolean() ? "GET" : "PUT";
                String location = "/" + random.nextInt();
                long status = random.nextBoolean() ? 200 : 400;
                long size = random.nextInt(200000);
                log.add("[" + dateStr + "] \"" + method + " " + location + " HTTP/1.1\" " + status + " " + size);
            }
        }

        return log;
    }
}

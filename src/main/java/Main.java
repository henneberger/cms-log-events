import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.google.common.collect.MinMaxPriorityQueue;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.reducing;
import java.util.stream.Stream;

/**
 * Created by henneberger on 8/20/17.
 */
public class Main {
    /**
     * Determines top K by streaming the results into memory and then counting
     */
    public List<LogResults> inMemoryCount(Stream<String> stream, int topK) {
        Map<String, LogCounter> counts = stream.map(LogBundle::parseLine)
                .filter(log -> "GET".equals(log.method))
                .filter(log -> log.status >= 200 && log.status < 300)
                .collect(Collectors.groupingBy(
                        f->f.location,
                            reducing(new LogCounter(0L, 0L), e -> new LogCounter(1L, e.size), //reduces w/ summing count & size
                                (f, g) -> new LogCounter(f.count + g.count, f.size + g.size))
                ));

        final Comparator<Map.Entry<String, LogCounter>> countComparator = Comparator.comparing(e->e.getValue().count);

        return counts.entrySet().stream()
                .sorted(countComparator.reversed())
                .limit(topK)
                .map(f->new LogResults(f.getKey(), f.getValue().size)) //unpack from map
                .collect(Collectors.toList());
    }

    /**
     * Uses count-min-sketches to determine log counts and sizes. Two sketches are used to not assume anything
     * about the distribution of the event data.
     */
    public List<LogResults> probabilisticCount(final Stream<String> stream, final int topK, final int depth,
                                               final int width, final int seed) {
        CountMinSketch cmsCount = new CountMinSketch(depth, width, seed);
        CountMinSketch cmsSize = new CountMinSketch(depth, width, seed);
        Comparator<TopKCounter> topKCounterComparator = Comparator.comparing(e->e.count);
        MinMaxPriorityQueue<TopKCounter> topKResults = MinMaxPriorityQueue.orderedBy(topKCounterComparator.reversed())
                .maximumSize(topK)
                .create();

        Stream<LogBundle> bundleStream = stream.map(LogBundle::parseLine)
                .filter(log -> "GET".equals(log.method))
                .filter(log -> log.status >= 200 && log.status < 300);

        bundleStream.forEach((f)->{
            cmsCount.add(f.location, 1);
            cmsSize.add(f.location, f.size);
            long estimatedCount = cmsCount.estimateCount(f.location);
            topKResults.remove(new TopKCounter(f.location, -1)); //remove any existing entry
            topKResults.offer(new TopKCounter(f.location, estimatedCount));
        });

        final Comparator<TopKCounter> countComparator = Comparator.comparing(e->e.count);

        return topKResults.stream()
                .sorted(countComparator.reversed())
                .map(f->new LogResults(f.location, cmsSize.estimateCount(f.location)))
                .collect(Collectors.toList());
    }

    public static void main(String args[]) {
        if (args.length != 2) {
            System.out.println("Finds the 10 most frequently requested objects and their cumulative bytes transferred. \n" +
                    "countdistinct.jar filename.log (deterministic/probabilistic)");
            return;
        }

        Main main = new Main();
        try {
            Stream<String> stream = Files.lines(Paths.get(args[0]));
            List<LogResults> results;
            if ("deterministic".equals(args[1])) {
                results = main.inMemoryCount(stream, 10);
            } else {
                results = main.probabilisticCount(stream, 10, 10, 10_000, 0);
            }

            for (LogResults result : results) {
                System.out.println(result.file + " " + result.size);
            }
        } catch (IOException e) {
            System.out.println("Could not find location " + args[0]);
            e.printStackTrace();
        };
    }
}

//just putting some basic objects here instead of a more sensible place
class LogBundle {
    private static Pattern LOG_REGEX = Pattern.compile("\\[(.*)\\] \"(.*) (.*) (.*)\" (.*) (.*)");
    public static DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");

    ZonedDateTime date;
    String method;
    String location;
    String httpVersion;
    int status;
    long size;

    public static LogBundle parseLine(final String s) {
        LogBundle logBundle = new LogBundle();
        Matcher matcher = LOG_REGEX.matcher(s);
        if (matcher.find()) {
            logBundle.date = ZonedDateTime.parse(matcher.group(1), DATE_FORMAT);
            logBundle.method = matcher.group(2);
            logBundle.location = matcher.group(3);
            logBundle.httpVersion = matcher.group(4);
            logBundle.status = Integer.parseInt(matcher.group(5));
            logBundle.size = Long.parseLong(matcher.group(6));
        } else {
            System.out.println("couldn't parse log line: " + s);
        }

        return logBundle;
    }
}

class LogCounter {
    final long count;
    final long size;

    public LogCounter(long count, long size) {
        this.count = count;
        this.size = size;
    }
}
class TopKCounter {
    final String location;
    final long count;

    public TopKCounter(String location, long count) {
        this.location = location;
        this.count = count;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final TopKCounter that = (TopKCounter) o;

        return location != null ? location.equals(that.location) : that.location == null;
    }

    @Override
    public int hashCode() {
        return location != null ? location.hashCode() : 0;
    }
}
class LogResults {
    final String file;
    final long size;

    public LogResults(String file, long size) {
        this.file = file;
        this.size = size;
    }

    @Override
    public String toString() {
        return "LogResults{" +
                "file='" + file + '\'' +
                ", size=" + size +
                '}';
    }
}
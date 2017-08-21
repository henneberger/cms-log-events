import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by henneberger on 8/20/17.
 */
public class MainTest {
    private Logger logger = Logger.getLogger(getClass().getName());
    private Main count;

    @Before
    public void setup() {
        this.count = new Main();
    }

    @Test
    public void testInMemory() throws IOException {
        int topK = 5;
        List<LogResults> results = count.inMemoryCount(getLocalFileStream(), topK);

        logger.info("Results " + results);
        assertEquals(results.get(0).uri, "/images/ksclogosmall.gif");
        assertEquals(results.get(0).size, 3635 * 3L);
    }

    @Test
    public void testProbabilistic() throws IOException {
        int topK = 5;
        int cmsDepth = 10;
        int cmdWidth = 100;
        List<LogResults> results = count.probabilisticCount(getLocalFileStream(), topK, cmsDepth, cmdWidth, 0);

        logger.info("Results " + results);
        assertEquals(results.get(0).uri, "/images/ksclogosmall.gif");
        assertEquals(results.get(0).size, 3635 * 3L);
    }

    Stream<String> getLocalFileStream() throws IOException {
        Path testFilePath = Paths.get(getClass().getClassLoader().getResource("CannedLogFile.log").getPath());

        return Files.lines(testFilePath);
    }


    @Test
    /**
     * Verify accuracy of probabilistic approach
     */
    public void accuracyTest() {
        int topK = 5;
        int cmsDepth = 10;
        int cmdWidth = 2_000;
        int numEvents = 200_000;
        List<String> logEvents = LogGenerator.randomEvents(numEvents, 0, 0.2); //Produce 20% random results to increase cardinality
        logger.info("Calculating results..");

        List<LogResults> inMemoryResults = count.inMemoryCount(logEvents.stream(), topK);
        List<LogResults> probabilisticResults = count.probabilisticCount(logEvents.stream(), topK, cmsDepth, cmdWidth, 0);

        logger.info("In-memory results " + inMemoryResults);
        logger.info("Probabili results " + probabilisticResults);

        double recall = calculateRecall(inMemoryResults, probabilisticResults);
        double precision = calculatePrecision(inMemoryResults, probabilisticResults);
        double f1 = calculateF1(recall, precision);

        logger.info("Recall: " + recall + " Precision: " + precision + " f1:" + f1);

        assertEquals(1, f1, 0.05); //verify f1 score is at least 0.95%

        for (int i = 0; i < 5; i++) {
            assertEquals(inMemoryResults.get(i).uri, probabilisticResults.get(i).uri);
            assertEquals(inMemoryResults.get(i).size, probabilisticResults.get(i).size, inMemoryResults.get(i).size * 0.05); //5% diff
        }
    }

    /**
     * How many of the results are contained in the actual set
     */
    public double calculateRecall(List<LogResults> truth, List<LogResults> results) {
        Set<String> s = truth.stream()
                .map(e->e.uri)
                .collect(Collectors.toSet());

        long recall = results.stream()
                .filter(e->s.contains(e.uri))
                .count();
        return recall / (double) truth.size();
    }

    /**
     * Weighted accuracy with count
     */
    public double calculatePrecision(List<LogResults> truth, List<LogResults> results) {
        Map<String, LogResults> mapped = Maps.uniqueIndex(truth, e->e.uri);
        double precision = 0;
        int resultCnt = 0;
        for (LogResults r : results) {
            LogResults t;
            if ((t = mapped.get(r.uri)) != null) {
                precision += Math.min(t.size, r.size) / (double)Math.max(t.size, r.size);
                resultCnt++;
            }
        }

        return precision / resultCnt;
    }

    /**
     * F1 score
     */
    private double calculateF1(double recall, double precision) {
        return 2 * ((precision * recall)/(precision + recall));
    }
}
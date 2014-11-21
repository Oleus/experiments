package bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class ContentmentBolt extends BaseBasicBolt {
    public static Logger logger = LoggerFactory.getLogger(ContentmentBolt.class);
    private final String positiveMask = ".*(good|great|awesome).*";
    private final String negativeMask = ".*(bad|terrible|awful).*";

    private String keyWord;
    private AtomicLong positive;
    private AtomicLong negative;

    public ContentmentBolt(String keyWord) {
        this.keyWord = keyWord;
        positive = new AtomicLong(0);
        negative = new AtomicLong(0);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        final String tweet = tuple.getStringByField("text");
        if (tweet.matches(positiveMask)) {
            positive.incrementAndGet();
            log();
        } else if (tweet.matches(negativeMask)) {
            negative.incrementAndGet();
            log();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    private void log() {
        logger.info("Contentment of " + keyWord + ": pos " + positive.get() + ", neg " + negative.get());
    }
}

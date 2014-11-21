package bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CompetingBolt extends BaseBasicBolt {
    public static Logger logger = LoggerFactory.getLogger(CompetingBolt.class);

    private String[] keyWords;
    private Map<String, AtomicLong> scores;

    public CompetingBolt(String[] keyWords) {
        this.keyWords = keyWords;
        scores = new HashMap<String, AtomicLong>(keyWords.length);
        for (String keyWord : keyWords) {
            scores.put(keyWord, new AtomicLong(0));
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        final String tweet = tuple.getStringByField("text");
        for (String keyWord : keyWords) {
            if (tweet.contains(keyWord)) {
                long score = scores.get(keyWord).incrementAndGet();
                logger.info(keyWord + " references: " + score);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}

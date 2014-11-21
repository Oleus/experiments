package twitparser.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitparser.model.Rating;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class RatingBolt extends BaseBasicBolt {
    public static Logger logger = LoggerFactory.getLogger(RatingBolt.class);
    private String id;
    private final String positiveMask = ".*(good|great|awesome|cool).*";
    private final String negativeMask = ".*(bad|terrible|awful).*";

    private Map<String, Rating> ratings;

    public RatingBolt(String[] keyWords) {
        ratings = new HashMap<String, Rating>(keyWords.length);
        for (String keyWord : keyWords) {
            ratings.put(keyWord, new Rating());
        }
        id = UUID.randomUUID().toString();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        final Rating rating = ratings.get(tuple.getStringByField("keyWord"));
        final String tweet = tuple.getStringByField("text");
        if (tweet.matches(positiveMask)) {
            rating.incrementPositive();
            log(tuple, rating);
        } else if (tweet.matches(negativeMask)) {
            rating.incrementNegative();
            log(tuple, rating);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    private void log(Tuple tuple, Rating rating) {
        logger.info("[bolt " + id + "] " +
                "Rating of " + tuple.getStringByField("keyWord") + ": " + rating.toString());
    }
}

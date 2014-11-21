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

public class RatingBolt extends BaseBasicBolt {
    public static Logger logger = LoggerFactory.getLogger(RatingBolt.class);
    private final String positiveMask = ".*(good|great|awesome|cool|honor|right|respect|benefi|adept|expert|safe|positive).*";
    private final String negativeMask = ".*(bad|terrible|awful|fear|negat|rubber|poor|unfavo|unsuit|horri|uncomfort|unfit|harm|evil).*";

    private Map<String, Rating> ratings;

    public RatingBolt(String[] keyWords) {
        ratings = new HashMap<String, Rating>(keyWords.length);
        for (String keyWord : keyWords) {
            ratings.put(keyWord, new Rating());
        }
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
        logger.info("Rating of " + tuple.getStringByField("keyWord") + ": " + rating.toString());
    }
}

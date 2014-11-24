package twitparser.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitparser.model.Rating;

public class RatingBolt extends BaseBasicBolt {
    public static Logger logger = LoggerFactory.getLogger(RatingBolt.class);
    private final String positiveMask = ".*(good|great|awesome|cool|honor|right|respect|benefi|adept|expert|safe|positive).*";
    private final String negativeMask = ".*(bad|terrible|awful|fear|negat|rubber|poor|unfavo|unsuit|horri|uncomfort|unfit|harm|evil).*";

    private Rating rating;
    private String keyWord;

    public RatingBolt(String keyWord) {
        this.keyWord = keyWord;
        rating = new Rating();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        final String tweet = tuple.getStringByField("text");
        if (tweet.matches(positiveMask)) {
            rating.incrementPositive();
            log();
        } else if (tweet.matches(negativeMask)) {
            rating.incrementNegative();
            log();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    private void log() {
        logger.info("Rating of " + keyWord + ": " + rating.toString());
    }
}

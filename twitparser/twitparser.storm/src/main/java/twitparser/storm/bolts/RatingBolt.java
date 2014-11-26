package twitparser.storm.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitparser.storm.Constants;
import twitparser.storm.models.Rating;

public class RatingBolt extends BaseBasicBolt {
    public static Logger logger = LoggerFactory.getLogger(RatingBolt.class);

    private Rating rating;
    private String keyWord;

    public RatingBolt(String keyWord) {
        this.keyWord = keyWord;
        rating = new Rating();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        final String tweet = tuple.getStringByField("text");
        if (tweet.matches(Constants.PositiveRatingMask)) {
            rating.incrementPositive();
            log();
        } else if (tweet.matches(Constants.NegativeRatingMask)) {
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

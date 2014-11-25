package twitparser.aggregators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.tuple.TridentTuple;
import twitparser.Constants;
import twitparser.models.Rating;

public class Rate implements storm.trident.operation.ReducerAggregator<Rating> {
    public static Logger logger = LoggerFactory.getLogger(Rate.class);

    @Override
    public Rating init() {
        return new Rating();
    }

    @Override
    public Rating reduce(Rating rating, TridentTuple tuple) {
        final String tweet = tuple.getStringByField("text");
        if (tweet.matches(Constants.PositiveRatingMask)) {
            rating.incrementPositive();
            log(rating, tuple);
        } else if (tweet.matches(Constants.NegativeRatingMask)) {
            rating.incrementNegative();
            log(rating, tuple);
        }
        return rating;
    }

    private void log(Rating rating, TridentTuple tuple) {
        logger.info("Rating of " + tuple.getStringByField("keyWord") + ": " + rating.toString());
    }
}

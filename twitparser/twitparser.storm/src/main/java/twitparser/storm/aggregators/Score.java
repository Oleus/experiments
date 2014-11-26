package twitparser.storm.aggregators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.tuple.TridentTuple;

public class Score implements storm.trident.operation.ReducerAggregator<java.lang.Long> {
    public static Logger logger = LoggerFactory.getLogger(Score.class);

    @Override
    public Long init() {
        return 1L;
    }

    @Override
    public Long reduce(Long curr, TridentTuple tuple) {
        long result = curr + 1;
        logger.info("Score for " + tuple.getString(0) + ": " + result);

        return result;
    }
}

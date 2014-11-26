package twitparser.storm.functions;

import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import twitter4j.Status;

public class Parse extends BaseFunction {
    public Logger logger = LoggerFactory.getLogger(Parse.class);

    private String[] keyWords;

    public Parse(String[] keyWords) {
        this.keyWords = keyWords;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        final Status status = (Status) tuple.getValueByField("tweet");
        final String tweet = status.getText().toLowerCase();
        for (String keyWord : keyWords) {
            if (tweet.contains(keyWord)) {
                collector.emit(new Values(keyWord, tweet));
                return;
            }
        }
        logger.info("Keywords were not found in the tweet: " + tweet);
    }
}

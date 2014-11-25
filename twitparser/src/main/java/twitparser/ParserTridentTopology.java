package twitparser;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.state.ValueUpdater;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import twitparser.spout.TwitterSpout;
import twitter4j.Status;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ParserTridentTopology {
    public static Logger logger = LoggerFactory.getLogger(ParserTridentTopology.class);

    public static void main(String[] args) throws Exception {
        String[] arguments = args.clone();
        String[] keyWords = Arrays.copyOfRange(arguments, 1, arguments.length);

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, buildTopology(keyWords));
        }
        else {
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology("test", conf, buildTopology(keyWords));

            Utils.sleep(10000);
            cluster.shutdown();
        }
    }

    private static StormTopology buildTopology(String[] keyWords) {
        TridentTopology topology = new TridentTopology();
        TwitterSpout spout = new TwitterSpout(keyWords);

        topology.newStream("twitterspout", spout)
                .parallelismHint(3)
                .each(new Fields("tweet"), new Parse(keyWords), new Fields("keyWord", "text"))
                .groupBy(new Fields("keyWord"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("keyWord"), new Score(), new Fields("score"));

        logger.info("All bolts were set for keywords: " + Arrays.toString(keyWords));

        return topology.build();
    }

    public static class Parse extends BaseFunction {
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

    public static class Score implements storm.trident.operation.ReducerAggregator<java.lang.Long> {
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
}

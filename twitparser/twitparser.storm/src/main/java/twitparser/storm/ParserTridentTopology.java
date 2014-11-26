package twitparser.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.TridentTopology;
import storm.trident.fluent.GroupedStream;
import storm.trident.testing.MemoryMapState;
import twitparser.storm.aggregators.Rate;
import twitparser.storm.aggregators.Score;
import twitparser.storm.functions.Parse;
import twitparser.storm.spouts.TwitterSpout;

import java.util.Arrays;

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

        GroupedStream keyWordsStream = topology.newStream("twitterspout", spout)
                .parallelismHint(4)
                .each(new Fields("tweet"), new Parse(keyWords), new Fields("keyWord", "text"))
                .groupBy(new Fields("keyWord"));

        keyWordsStream.persistentAggregate(new MemoryMapState.Factory(), new Fields("keyWord"), new Score(), new Fields("score"));
        keyWordsStream.persistentAggregate(new MemoryMapState.Factory(), new Fields("keyWord", "text"), new Rate(), new Fields("rating"));

        logger.info("All com.twitparser.storm.bolts were set for keywords: " + Arrays.toString(keyWords));

        return topology.build();
    }
}

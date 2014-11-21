package twitparser;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import twitparser.bolt.GroupingBolt;
import twitparser.bolt.RatingBolt;
import twitparser.bolt.ParsingBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitparser.spout.TwitterSpout;

import java.util.Arrays;

public class ParserStream {
    public static Logger logger = LoggerFactory.getLogger(ParserStream.class);

    public static void main(String[] args) throws Exception {
        String[] arguments = args.clone();
        String[] keyWords = Arrays.copyOfRange(arguments, 1, arguments.length);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("twitter", new TwitterSpout(keyWords));
        builder.setBolt("parse", new ParsingBolt()).shuffleGrouping("twitter");
        builder.setBolt("group", new GroupingBolt(keyWords)).shuffleGrouping("parse");
        builder.setBolt("rating", new RatingBolt(keyWords)).fieldsGrouping("group", new Fields("keyWord"));
        logger.info("All bolts were set for keywords: " + Arrays.toString(keyWords));

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology("test", conf, builder.createTopology());

            Utils.sleep(10000);
            cluster.shutdown();
        }
    }
}

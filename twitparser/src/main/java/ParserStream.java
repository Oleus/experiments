import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import bolt.CompetingBolt;
import bolt.ContentmentBolt;
import bolt.ParsingBolt;
import bolt.PrinterBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spout.TwitterSpout;

import java.util.Arrays;

public class ParserStream {
    public static Logger logger = LoggerFactory.getLogger(ParserStream.class);

    public static void main(String[] args) throws Exception {
        String consumerKey = "";
        String consumerSecret = "";
        String accessToken = "";
        String accessTokenSecret = "";
        String[] arguments = args.clone();
        String[] keyWords = Arrays.copyOfRange(arguments, 1, arguments.length);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("twitter", new TwitterSpout(consumerKey, consumerSecret,
                accessToken, accessTokenSecret, keyWords));
        builder.setBolt("parse", new ParsingBolt()).shuffleGrouping("twitter");
        builder.setBolt("compete", new CompetingBolt(keyWords)).shuffleGrouping("parse");
        for (String keyWord : keyWords) {
            builder.setBolt(keyWord, new ContentmentBolt(keyWord)).shuffleGrouping("parse");
        }
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

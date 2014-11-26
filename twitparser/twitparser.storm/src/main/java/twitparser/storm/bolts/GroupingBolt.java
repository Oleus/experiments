package twitparser.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class GroupingBolt extends BaseRichBolt {
    public static Logger logger = LoggerFactory.getLogger(GroupingBolt.class);
    private OutputCollector collector;

    private String[] keyWords;
    private Map<String, AtomicLong> scores;

    public GroupingBolt(String[] keyWords) {
        this.keyWords = keyWords;
        scores = new HashMap<String, AtomicLong>(keyWords.length);
        for (String keyWord : keyWords) {
            scores.put(keyWord, new AtomicLong(0));
        }
    }

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        final String tweet = tuple.getStringByField("text");
        for (String keyWord : keyWords) {
            //keyWord can be not found for different languages
            if (tweet.contains(keyWord)) {
                long score = scores.get(keyWord).incrementAndGet();
                logger.info(keyWord + " references: " + score);
                collector.emit(keyWord, new Values(tweet));
                return;
            }
        }
        logger.info("Keywords were not found in the tweet: " + tweet);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (String keyWord : keyWords) {
            declarer.declareStream(keyWord, true, new Fields("text"));
        }
//        declarer.declare(new Fields("keyWord", "text"));
    }
}

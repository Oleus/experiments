package twitparser.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

import java.util.Map;

public class ParsingBolt extends BaseRichBolt {
    public static Logger logger = LoggerFactory.getLogger(ParsingBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        final Status status = (Status) tuple.getValueByField("tweet");
        final String text = status.getText().toLowerCase();
//        logger.info("A tweet '" + status.getId() + "' parsed: " + (text.length() > 20 ? text.substring(0, 20) : text));
        collector.emit(new Values(text));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text"));
    }
}

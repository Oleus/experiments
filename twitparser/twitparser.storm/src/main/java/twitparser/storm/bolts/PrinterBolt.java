package twitparser.storm.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrinterBolt extends BaseBasicBolt {
    public static Logger logger = LoggerFactory.getLogger(PrinterBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        logger.info("A tweet received: ", tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}

package twitparser.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

import java.util.AbstractMap;
import java.util.Arrays;

public class SparkStreamingApplication {
    public static void main(String[] args) {
        final String[] arguments = args.clone();
        final String[] keyWords = Arrays.copyOfRange(arguments, 1, arguments.length);

        SparkConf config = new SparkConf().setMaster("local[2]").setAppName("Twitparser");
        JavaStreamingContext context = new JavaStreamingContext(config, new Duration(1000));

        TwitterUtils.createStream(context)
                .map(new Function<Status, String>() {
                         @Override
                         public String call(Status status) throws Exception {
                             return status.getText();
                         }
                     }
                );
    }
}

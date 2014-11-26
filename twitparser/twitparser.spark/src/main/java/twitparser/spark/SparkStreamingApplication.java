package twitparser.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.Arrays;

public class SparkStreamingApplication {
    public static void main(String[] args) {
        final String[] arguments = args.clone();
        final String[] keyWords = Arrays.copyOfRange(arguments, 1, arguments.length);

        SparkConf config = new SparkConf().setMaster("local[2]").setAppName("Twitparser");
        JavaStreamingContext context = new JavaStreamingContext(config, new Duration(1000));
        Configuration twitterConfig = new ConfigurationBuilder()
                .setJSONStoreEnabled(true)
                .setOAuthConsumerKey(Constants.ConsumerKey)
                .setOAuthConsumerSecret(Constants.ConsumerSecret)
                .setOAuthAccessToken(Constants.AccessToken)
                .setOAuthAccessTokenSecret(Constants.AccessTokenSecret)
                .build();

        TwitterUtils.createStream(context, new OAuthAuthorization(twitterConfig), keyWords)
                .map(new Function<Status, String>() {
                         @Override
                         public String call(Status status) throws Exception {
                             return status.getText().toLowerCase();
                         }
                     }
                )
                .flatMapToPair(new PairFlatMapFunction<String, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(String tweet) throws Exception {
                        ArrayList<Tuple2<String, String>> result = new ArrayList<Tuple2<String, String>>();

                        for (String keyWord : keyWords) {
                            if (tweet.contains(keyWord)) {
                                result.add(new Tuple2<String, String>(keyWord, tweet));
                                return result;
                            }
                        }

                        System.out.println("Keywords were not found in the tweet: " + tweet);
                        return result;
                    }
                });
    }
}

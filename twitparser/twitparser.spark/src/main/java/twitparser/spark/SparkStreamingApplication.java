package twitparser.spark;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkStreamingApplication {
    public static void main(String[] args) {
        final String[] arguments = args.clone();
        final String[] keyWords = Arrays.copyOfRange(arguments, 1, arguments.length);

        SparkConf config = new SparkConf().setMaster("local[2]").setAppName("Twitparser");
        JavaStreamingContext context = new JavaStreamingContext(config, new Duration(1000));


        JavaPairDStream<String, Iterable<String>> groupedTweets = parseTweets(context, keyWords);

    }

    private static JavaPairDStream<String, Iterable<String>> parseTweets(JavaStreamingContext context, final String[] keyWords) {
        Configuration twitterConfig = new ConfigurationBuilder()
                .setJSONStoreEnabled(true)
                .setOAuthConsumerKey(Constants.ConsumerKey)
                .setOAuthConsumerSecret(Constants.ConsumerSecret)
                .setOAuthAccessToken(Constants.AccessToken)
                .setOAuthAccessTokenSecret(Constants.AccessTokenSecret)
                .build();

        return TwitterUtils.createStream(context, new OAuthAuthorization(twitterConfig), keyWords)
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
                })
                .groupByKey();
    }

    private static void countTweets(JavaPairDStream<String, String> groupedTweets) {
        groupedTweets.updateStateByKey(new Function2<List<String>, Optional<Integer>, Optional<Integer>>() {
                    @Override
                    public Optional<Integer> call(List<String> strings, Optional<Integer> optional) throws Exception {
                        Integer count = new Integer(0);
                        if (optional.isPresent()) {
                            count = optional.get();
                        }
                        count += strings.size();
                        return Optional.of(count);
                    }
                })
                .foreach(new Function<JavaPairRDD<String, Integer>, Void>() {
                    @Override
                    public Void call(JavaPairRDD<String, Integer> rdd) throws Exception {
                        rdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                            @Override
                            public void call(Tuple2<String, Integer> tuple) throws Exception {
                                System.out.println("Score of "+ tuple._1() + ": " + tuple._2());
                            }
                        });
                        return null;
                    }
                });
    }

    private static void rateTweets(JavaPairDStream<String, String> groupedTweets) {

    }
}

package streams;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;


/**
 * Kafka Streams - Easy data processing and transformation library
 * This stream extracts the followers in tweets and sends the message to 'important-tweets' topic
 *
 * A simple kafka stream
 */
@Slf4j
public class TweetsFilterStream {


    public static void main(String[] args) {

        final Properties properties = getProperties();

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");

        final KStream<String,String> filteredStream = inputTopic.filter((key,  value) ->  extractUserFollowersInTweet(value) > 1000);

        filteredStream.to("important-tweets");

        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties
        );

        log.info("Kafka Streams start");
        kafkaStreams.start();
    }

    private static Integer extractUserFollowersInTweet(String tweetJson){

        JsonParser jsonParser = new JsonParser();
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }
        catch (NullPointerException e){
            return 0;
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams"); // stream application id
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        return properties;
    }
}

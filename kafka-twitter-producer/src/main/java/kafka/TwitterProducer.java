package kafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * A simple Twitter Producer - reads tweets for hashtags
 */

@Slf4j
public class TwitterProducer {

    private static final String CONSUMER_KEY = "#####use your consumer key ######";
    private static final String CONSUMER_SECRET = "use your consumer secret";
    private static final String TOKEN = "user your token";
    private static final String SECRET = "user your secret";

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    /**
     * Run method
     */
    public void run() {

        log.info("Into run method");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10000);

        List<String> terms = Lists.newArrayList("india","usa");

        Client client = this.createTwitterClient(msgQueue, terms);
        client.connect();

        //create a kafka producer
        KafkaProducer<String, String> kafkaProducer = this.createKafkaProducer();


        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            log.info("stopping application");
            log.info("shutting down client producer");
            client.stop();
            log.info("Client closed");
            log.info("Closing producer");
            kafkaProducer.close();
            log.info("Producer closed");

        }));


        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                log.info(msg);

                //make sure that "twitter_tweets" topic is created before
                //on Terminal: kafka-topics --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1
                //For Testing run this consumer command : kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic twitter_tweets
                kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), (RecordMetadata recordMetadata, Exception e) -> {
                    if (e != null) {
                        log.error("Error while sending tweets");
                    }
                });
            }
        }
        log.info("End of application");
    }


    /**
     * Create Kafka Producer
     *
     * @return
     */
    private KafkaProducer<String, String> createKafkaProducer() {

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create a safe producer - settings
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");


        // high through put settings
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //32-Kb

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }


    /**
     * create twitter client
     *
     * @param msgQueue
     * @return
     */
    private Client createTwitterClient(BlockingQueue<String> msgQueue, List<String> terms) {

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}

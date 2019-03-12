package com.kafka.experiments.consumer;


import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ElasticSearchConsumer {


    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = ElasticSearcheRestClientConnection.createClient();

        KafkaConsumer<String, String> consumer = createConsumer();

        //String jsonString = "{\"firstName\":\"Joe\",\"lastName\":\"Shmo\"}";

        BulkRequest bulkRequest = new BulkRequest();


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            int count = records.count();

            log.info("Received Records ::" + count + " records");

            for (ConsumerRecord record : records) {

                //Idempotence can be achieved in 2 ways
                // generate kafka generic id
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                //or generate a unique id

                String id = extractIdFromTweet((String) record.value()); //for make sure that our consumer is idempotent


                IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id)
                        .source(((String) record.value()).replace('.', ' '), XContentType.JSON);

                //IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

                bulkRequest.add(indexRequest);
                //log.info("Response Id :: " + indexResponse.getId());

                //sleep();// just adding small delay
            }


            if (count > 0) {
                try {
                    BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Committing offsets ");
                } catch (Exception e) {
                    // In case there is an exception, we would like to continue.
                    log.error("Exception inserting into elastic search because of bad data :: ");
                }
                consumer.commitAsync();


                log.info("Offsets have been committed");
                sleep();// just adding small delay
            }


        }
        //client.close();
    }


    /**
     * Create Consumer
     *
     * @return
     */
    public static KafkaConsumer<String, String> createConsumer() {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-elastic-search";
        String topic = "twitter_tweets";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit

        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50"); // only 10 records at a time

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;

    }


    private static void sleep() {
        try {
            Thread.sleep(1000); // just adding small delay
        } catch (InterruptedException e) {
            log.error("Interrupt Exception", e);
        }
    }


    private static String extractIdFromTweet(String tweet) {
        JsonParser jsonParser = new JsonParser();
        String idStr = jsonParser.parse(tweet)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
        return idStr;
    }

}

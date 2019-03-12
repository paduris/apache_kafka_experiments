package kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ProducerDemoWithKeys {

    public static final String NEXT_LINE = "\n";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10 ; i++) {

            String topic ="first_topic";
            String message ="hello, world from Suresh - new message " + new Date();
            String key = "id_" + Integer.toString(i);


            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key , message);

            // NOTE: providing a key will always guarantee that same key will go to same partition - Research

            log.info("Key :: " + key);

            producer.send(record, (metadata, exception) -> {

                if (exception == null) {
                    StringBuilder builder = new StringBuilder();

                    builder.append("Received latest meta data" + NEXT_LINE);
                    builder.append("Topic :" + metadata.topic() + NEXT_LINE);
                    builder.append("Partition :" + metadata.partition() + NEXT_LINE);
                    builder.append("Offset: " + metadata.offset() + NEXT_LINE);
                    builder.append("Timestamp :" + metadata.timestamp());

                    log.info(builder.toString());

                } else {
                    log.error("Exception while producing message", exception);
                }
            }).get(); // block the .send to make it synchronous - don't do this in production. This is just for understanding/testing purposes
        }

        producer.flush();
        producer.close();
    }
}

package kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;

@Slf4j
public class ProducerDemoWithCallBack {

    public static final String NEXT_LINE = "\n";

    /**
     * Main method
     *
     * @param args
     */
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello, world from Suresh - new message " + new Date());


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

        });
        producer.flush();
        producer.close();
    }
}

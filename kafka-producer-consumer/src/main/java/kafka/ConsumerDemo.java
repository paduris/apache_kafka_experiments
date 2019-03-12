package kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


/**
 * Simple Consumer Demo
 */
@Slf4j
public class ConsumerDemo {


    /**
     * Main method
     *
     * @param args
     */
    public static void main(String[] args) {

        final String bootstrapServers = "127.0.0.1:9092";
        final String groupId = "my-fourth-application"; // consumer group

        final Properties props = getProperties(bootstrapServers, groupId);

        final KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList("first_topic","second_topic"));

        while(true)
        {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord record : records)
            {
                log.info("Key :: " + record.key() + " Value :: " + record.value());
                log.info("Partition :: " + record.partition() + " Offset :: " + record.offset());
            }
        }
    }


    /**
     *
     * @param bootstrapServers
     * @param groupId
     * @return
     */
    private static Properties getProperties(String bootstrapServers, String groupId) {
        Properties props  = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        return props;
    }
}

package kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


/**
 * A Demo shows soft exit - shutting down gracefully using thread implementation
 *
 */
@Slf4j
public class ConsumerDemoWithThread {


    private static final String bootstrapServers = "127.0.0.1:9092";
    private static final String groupId = "my-fourth-application";
    private static final String topic = "first_topic";


    /**
     * Main method
     *
     * @param args
     */
    public static void main(String[] args) {

        ConsumerDemoWithThread consumerDemoWithThread = new ConsumerDemoWithThread();
        consumerDemoWithThread.run();
    }


    /* No argument constructor */
    public ConsumerDemoWithThread() {
        log.info("Into no argument constructor");
    }


    /*
     * Run method
     */
    private void run() {

        CountDownLatch latch = new CountDownLatch(1);

        Runnable runnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);

        Thread myThread = new Thread(runnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            log.info("Caught shutdown hook");

            ((ConsumerRunnable) runnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Application has exited");
        }));


        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Application got interrupted", e);
        } finally {
            log.info("Application is closing");
        }
    }


    /**
     * Consumer Runnable
     */
    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;


        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {

            this.latch = latch;
            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<String, String>(props);
            consumer.subscribe(Arrays.asList(topic));

        }

        @Override
        public void run() {

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord record : records) {
                        log.info("Key ::" + record.key() + " Value ::" + record.value());
                        log.info("Partition ::" + record.partition() + " Offset ::" + record.offset());
                    }
                }
            } catch (WakeupException e) {
                log.info("Received wakeup signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }


        public void shutdown() {
            consumer.wakeup();

        }

    }
}

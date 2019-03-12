package com.kafka.spring;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootApplication
public class KafkaProducerConsumerSpringApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(KafkaProducerConsumerSpringApplication.class, args);
	}

	@Autowired
	private KafkaTemplate<String, String> template;

	private CountDownLatch latch = new CountDownLatch(1000);

	@Override
	public void run(String... args) throws Exception {

		log.info("Into run method");

		for (int i = 0; i < 5; i++) {
			this.template.send("first_topic","This message is coming from a spring program " + String.valueOf(i));
		}
		latch.await(60, TimeUnit.MILLISECONDS);

		log.info("Exiting the run method");
	}

	@KafkaListener(topics = "first_topic" , groupId = "my-fourth-application")
	public void listen(ConsumerRecord<String, String> consumer)
	{
		log.info("Consuming record :: "+ consumer.value());
		latch.countDown();
	}
}

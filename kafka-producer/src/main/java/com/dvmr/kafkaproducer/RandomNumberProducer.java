package com.dvmr.kafkaproducer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;


@Component
public class RandomNumberProducer {
	private static final Logger log = LoggerFactory.getLogger(RandomNumberProducer.class);
	private static final int MIN = 10;
	private static final int MAX = 100_000;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Scheduled(fixedRate = 1000)
	public void produce() throws UnknownHostException {
		int message = ThreadLocalRandom.current().nextInt(MIN, MAX);
		ListenableFuture<SendResult<String, String>> future = 
				this.kafkaTemplate.sendDefault(String.valueOf(message));
		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

			@Override
			public void onSuccess(SendResult<String, String> result) {
				log.info("Kafka sent message='{}' with offset={}", message,
		                result.getRecordMetadata().offset());
				
			}

			@Override
			public void onFailure(Throwable ex) {
				log.error("Kafka unable to send message='{}'", message, ex);
				
			}
		});
		// just for logging
		String hostName = InetAddress.getLocalHost().getHostName();
		log.info("{} produced {}", hostName, message);
	}

}

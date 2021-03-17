package com.dvmr.kafkaconsumer;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class RandomNumberConsumer {

	@KafkaListener(topics = "random-number")
    public void consume(String message) throws UnknownHostException {
        String hostName = InetAddress.getLocalHost().getHostName();
        System.out.println(String.format("%s consumed %s", hostName, message));
    }
}

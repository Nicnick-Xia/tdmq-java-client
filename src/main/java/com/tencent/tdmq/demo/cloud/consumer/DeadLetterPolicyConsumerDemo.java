package com.tencent.tdmq.demo.cloud.consumer;

import com.tencent.tdmq.demo.cloud.Config;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadLetterPolicyConsumerDemo {
	private static final Logger logger = LoggerFactory.getLogger(DeadLetterPolicyConsumerDemo.class);

	public static void main(String[] args) throws PulsarClientException {
		try (PulsarClient client = PulsarClient.builder()
				.serviceUrl(Config.SERVICE_URL)
				.authentication(AuthenticationFactory.token(Config.TOKEN))
				.build()) {

			Consumer<byte[]> consumer = client.newConsumer()
					.topic(Config.TOPIC)
					.subscriptionName(Config.SUBSCRIPTION + "-dlq")
					.negativeAckRedeliveryDelay(1000, java.util.concurrent.TimeUnit.MILLISECONDS)
					.deadLetterPolicy(DeadLetterPolicy.builder()
							.maxRedeliverCount(3)
							.deadLetterTopic(Config.TOPIC + "-DLQ")
							.build())
					.subscribe();
			logger.info("{}", ">> dlq consumer created.");

			for (int i = 0; i < 10; i++) {
				Message<byte[]> msg = consumer.receive();
				try {
					throw new RuntimeException("simulate processing failure");
				} catch (Exception e) {
					logger.info("processing failed for msg {}, will nack", msg.getMessageId());
					consumer.negativeAcknowledge(msg);
				}
			}
			consumer.close();
		}
	}
}
package com.tencent.tdmq.demo.cloud.consumer;

import com.tencent.tdmq.demo.cloud.Config;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeySharedConsumerDemo {
	private static final Logger logger = LoggerFactory.getLogger(KeySharedConsumerDemo.class);

	public static void main(String[] args) throws PulsarClientException {
		try (PulsarClient client = PulsarClient.builder()
				.serviceUrl(Config.SERVICE_URL)
				.authentication(AuthenticationFactory.token(Config.TOKEN))
				.build()) {

			Consumer<byte[]> consumer = client.newConsumer()
					.topic(Config.TOPIC)
					.subscriptionName(Config.SUBSCRIPTION + "-keyshared")
					.subscriptionType(SubscriptionType.Key_Shared)
					.subscribe();
			logger.info("{}", ">> key_shared consumer created.");

			for (int i = 0; i < 10; i++) {
				Message<byte[]> msg = consumer.receive();
				try {
					logger.info("key={}, value={}", msg.getKey(), new String(msg.getValue()));
					consumer.acknowledge(msg);
				} catch (Exception e) {
					consumer.negativeAcknowledge(msg);
				}
			}
			consumer.close();
		}
	}
}
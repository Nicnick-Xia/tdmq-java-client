package com.tencent.tdmq.demo.cloud;

import junit.framework.TestCase;

public class ConfigTest extends TestCase {
	public void testTopicStartsWithPersistent() {
		assertNotNull(Config.TOPIC);
		assertTrue(Config.TOPIC.startsWith("persistent://"));
	}

	public void testSubscriptionNotEmpty() {
		assertNotNull(Config.SUBSCRIPTION);
		assertTrue(Config.SUBSCRIPTION.trim().length() > 0);
	}

	public void testTokenNotEmpty() {
		assertNotNull(Config.TOKEN);
		assertTrue(Config.TOKEN.trim().length() > 0);
	}

	public void testServiceUrlProtocol() {
		assertNotNull(Config.SERVICE_URL);
		String url = Config.SERVICE_URL.trim().toLowerCase();
		assertTrue(url.startsWith("http://") || url.startsWith("https://"));
	}

	public void testServiceUrlHasHostAndPort() {
		String url = Config.SERVICE_URL;
		assertTrue(url.contains("://"));
		// crude host:port presence check after protocol
		String afterProtocol = url.substring(url.indexOf("://") + 3);
		assertTrue(afterProtocol.contains(":"));
	}
}
package com.tencent.tdmq.demo.cloud.consumer;

import junit.framework.TestCase;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class ConsumersSmokeTest extends TestCase {
	public void testSimpleConsumerHasMain() throws Exception {
		Method m = SimpleConsumer.class.getDeclaredMethod("main", String[].class);
		assertTrue(Modifier.isPublic(m.getModifiers()));
		assertTrue(Modifier.isStatic(m.getModifiers()));
	}

	public void testBatchConsumerHasMain() throws Exception {
		Method m = BatchConsumer.class.getDeclaredMethod("main", String[].class);
		assertTrue(Modifier.isPublic(m.getModifiers()));
		assertTrue(Modifier.isStatic(m.getModifiers()));
	}
}
package com.tencent.tdmq.demo.cloud.consumer;

import com.tencent.tdmq.demo.cloud.Config;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {
    private static final Logger log = LoggerFactory.getLogger(SimpleConsumer.class);

    public static void main(String[] args) throws PulsarClientException {
        invoke();
    }

    private static void invoke() throws PulsarClientException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(Config.SERVICE_URL)
                .authentication(AuthenticationFactory.token(Config.TOKEN))
                .build();
        log.info("pulsar client created");

        //创建消费者
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(Config.TOPIC)
                .subscriptionName(Config.SUBSCRIPTION)
                //声明消费模式，默认值为 SubscriptionType.Exclusive
                //可以选择订阅模式包括
                //Exclusive 独占模式
                //Failover 灾备模式（独占模式的主备模式）
                //Shared 共享模式
                //Key_Shared 按照Key分类的共享模式（同一个key的消息会发往固定的partition和消费者）
                .subscriptionType(SubscriptionType.Exclusive)
                //配置从最早开始消费，否则可能会消费不到历史消息
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        log.info("pulsar consumer created");

        for (int i = 0; i < 10; i++) {
            // 等待接收到下一条消息
            Message<byte[]> msg = consumer.receive();
            MessageId msgId = msg.getMessageId();
            String value= new String(msg.getValue());
            try {
                // TODO:对消息进行处理
                log.info("receive msg {}, value:{}", msgId, value);
                // 消费者确认收到消息
                consumer.acknowledge(msg);
            } catch (Exception e) {
                log.warn("processing failed for msgId {}: {}", msgId, e.toString());
                consumer.negativeAcknowledge(msg);
            }
        }
        consumer.close();
        client.close();
    }
}

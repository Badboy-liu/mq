package com.example.comsumer.wrapper;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;
import java.util.Set;

/**
 * @Author: qinglai
 * @Date: 2021/12/31 15:04
 */
public abstract class MqConsumerWrapper {
    public static DefaultMQPushConsumer getPushConsumer(String consumerGroup, String addr, String topic,String expression,MessageListenerOrderly listenerOrderly) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(addr);
        consumer.subscribe(topic,expression);
        consumer.registerMessageListener(listenerOrderly);
        consumer.start();
        return consumer;
    }


}

package com.example.producer.wrapper;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;

/**
 * @Author: qinglai
 * @Date: 2021/12/31 14:52
 */
public abstract class MqProducerWrapper {
    public static DefaultMQProducer getMQProducer(String producerGroup,String Addr) throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(Addr);
        producer.start();
        return producer;
    }
}

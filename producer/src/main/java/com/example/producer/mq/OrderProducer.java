package com.example.producer.mq;

import com.example.common.constant.MqConstant;
import com.example.producer.wrapper.MessageWrapper;
import com.example.producer.wrapper.MqProducerWrapper;
import com.google.common.collect.Lists;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @Author: qinglai
 * @Date: 2021/12/31 15:31
 */
public class OrderProducer {
    public static void main(String[] args) throws Exception {
        orderBatchTag();
    }

    private static void orderBatchTag() throws MQClientException, UnsupportedEncodingException, RemotingException, MQBrokerException, InterruptedException {
        // 构建生产者
        DefaultMQProducer producer = MqProducerWrapper.getMQProducer(MqConstant.GROUP, MqConstant.ADDR);
        List<String> tags = Lists.newArrayList("tagA", "tagB", "tagC");

        // 30个订单
        int num = 30;

        for (int i = 0; i < num; i++) {
            int orderId = i % 3;
            Message message = MessageWrapper.getMessage(MqConstant.TOPIC, tags.get(i % tags.size()), "订单编号:" + i);
            producer.send(message, (mqs, msg, arg) -> mqs.get((Integer) (arg)%mqs.size()), orderId);}
        producer.shutdown();
    }

}

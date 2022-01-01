package com.example.comsumer.mq;

import com.example.comsumer.wrapper.MqConsumerWrapper;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @Author: qinglai
 * @Date: 2021/12/31 10:52
 */
public class ConsumerMq {
    public static void main(String[] args) throws Exception {
//        pushConsumer();
        pushConsumer2();

    }

    private static void pushConsumer2() throws Exception {
        DefaultMQPushConsumer consumer = MqConsumerWrapper.getPushConsumer("blacksnake", "http://120.79.128.25:8012", "year","*", new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {


                try {
                    for (MessageExt msg : msgs) {
                        String text = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        System.out.println(text);
                    }
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
    }

    private static void pushConsumer() throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("blacksnake");
        consumer.setNamesrvAddr("http://120.79.128.25:8012");
        consumer.subscribe("year", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
//            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
            for (MessageExt msg : msgs) {
                try {
                    String text = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                    System.out.println(text);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
    }
}

package com.example.producer.mq;

import com.example.common.constant.MqConstant;
import com.example.producer.wrapper.MessageWrapper;
import com.example.producer.wrapper.MqProducerWrapper;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ProducerMq {

    public static void main(String[] args) throws Exception{
        // 同步
//        syn();
        // 异步
//        asyn();
        // 单次
//        one();
        // 批量
        batch();
    }

    /**
     * 批量发送
     * @throws Exception
     */
    private static void batch() throws Exception {
        DefaultMQProducer producer = MqProducerWrapper.getMQProducer(MqConstant.GROUP, MqConstant.ADDR);
        Message message = MessageWrapper.getMessage(MqConstant.TOPIC,MqConstant.TAG,"一次发送");
        Message messageTwo = MessageWrapper.getMessage(MqConstant.TOPIC,MqConstant.TAG,"二次发送");
        List<Message> messages = MessageWrapper.getMessages(message,messageTwo);
        producer.send(messages);
    }

    /**
     * 单次发送
     * @throws Exception
     */
    private static void one() throws Exception{
        DefaultMQProducer producer = MqProducerWrapper.getMQProducer(MqConstant.GROUP,MqConstant.ADDR);
        Message message = MessageWrapper.getMessage(MqConstant.TOPIC,MqConstant.TAG,"一次发送");
        producer.sendOneway(message);
    }

    /**
     * 同步
     * @throws Exception
     */
    private static void syn() throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer(MqConstant.GROUP);
        producer.setNamesrvAddr(MqConstant.ADDR);
        producer.start();
        for (int i = 0; i < 100; i++) {

            Message message = new Message(MqConstant.TOPIC,MqConstant.TAG,"2021","过年快乐".getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }
    }

    /**
     * 异步
     * @throws Exception
     */
    public static void asyn() throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer(MqConstant.GROUP);
        producer.setNamesrvAddr(MqConstant.ADDR);
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
            Message message = MessageWrapper.getMessage();
            message.setTopic(MqConstant.TOPIC);
            message.setTags(MqConstant.TAG);
            message.setKeys("2020");
            message.setBody("异步".getBytes(RemotingHelper.DEFAULT_CHARSET));

            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println("发送成功");
                }

                @Override
                public void onException(Throwable e) {
                    System.out.println("发送异常");
                    e.printStackTrace();
                }
            });
            countDownLatch.await(5, TimeUnit.SECONDS);
            producer.shutdown();

    }

}

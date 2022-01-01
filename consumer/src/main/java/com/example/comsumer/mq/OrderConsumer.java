package com.example.comsumer.mq;

import com.example.common.constant.MqConstant;
import com.example.comsumer.wrapper.MqConsumerWrapper;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: qinglai
 * @Date: 2021/12/31 15:56
 */
public class OrderConsumer {
    public static void main(String[] args) throws Exception {
        orderConsumer("*");
//        Thread.sleep(1000);
//        orderConsumer("*");
//        Thread.sleep(1000);
//        orderConsumer("*");
    }

    private static void orderConsumer(String expression) throws MQClientException {
        AtomicLong atomicLong = new AtomicLong(0);
        DefaultMQPushConsumer consumer = MqConsumerWrapper
                .getPushConsumer(
                        MqConstant.GROUP,
                        MqConstant.ADDR,
                        MqConstant.TOPIC,
                        expression,
                        (msgs, context) -> {
                            context.setAutoCommit(false);
                            atomicLong.incrementAndGet();
                            for (MessageExt msg : msgs) {
                                try {
                                    String text = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                                    System.out.println(text+"  long:"+atomicLong.get());
                                    if (atomicLong.get() % 2 == 0) {
                                        return ConsumeOrderlyStatus.SUCCESS;
                                    } else if (atomicLong.get() % 3 == 0) {
                                        return ConsumeOrderlyStatus.ROLLBACK;
                                    } else if (atomicLong.get() % 10 == 0) {
                                        return ConsumeOrderlyStatus.COMMIT;
                                    } else if (atomicLong.get() % 5 == 0) {
                                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                                    }
                                } catch (UnsupportedEncodingException e) {
                                    e.printStackTrace();
                                }
                            }
                            return ConsumeOrderlyStatus.SUCCESS;
                        });
    }
}

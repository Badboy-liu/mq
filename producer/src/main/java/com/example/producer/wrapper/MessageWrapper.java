package com.example.producer.wrapper;

import com.google.common.collect.Lists;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @Author: qinglai
 * @Date: 2021/12/31 14:25
 */
public abstract class MessageWrapper {

    public static Message getMessage(){
        return new Message();
    }
    public static Message getMessage(String topic,String tag,String body) throws UnsupportedEncodingException {
        return new Message(topic,tag,body.getBytes(RemotingHelper.DEFAULT_CHARSET));
    }

    public static List<Message> getMessages(Message... messages){
        return Lists.newArrayList(messages);
    }

}

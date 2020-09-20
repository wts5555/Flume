package com.wts.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

public class MyInterceptor implements Interceptor {
    //初始化方法
    @Override
    public void initialize() {

    }

    //拦截方法，处理一个Event
    @Override
    public Event intercept(Event event) {
        //获取heder
        Map<String, String> headers = event.getHeaders();
        //获取Body
       // byte[] body = event.getBody();
       // String str = body.toString();
        String str = new String(event.getBody());
        //判断body中是否包含wts
        if (str.contains("wts")) {
            //在header中添加kv
            headers.put("flag", "wts");
        } else {
            headers.put("flag", "other");
        }
        return event;
    }

    //拦截方法 处理批量的Event
    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    //关闭资源的方法
    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder {

        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        //获取flume中配置文件的属性
        @Override
        public void configure(Context context) {

        }
    }
}

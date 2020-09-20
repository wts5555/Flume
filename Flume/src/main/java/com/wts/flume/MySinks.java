package com.wts.flume;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySinks extends AbstractSink implements Configurable {

    //创建Logger对象
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSink.class);
    private String prefix;
    private String suffix;

    @Override
    public Status process() throws EventDeliveryException {
        //声明返回值状态信息
        Status status;
        //获取当前sink绑定的channel
        Channel ch = getChannel();
        //获取事物
        Transaction txn = ch.getTransaction();
        //声明事件
        Event event;
        //开启事物
        txn.begin();
        //读取channel中的事件，直到读取到事件结束循环
        while (true) {
            event = ch.take();
            if (event != null) {
                break;
            }
        }
        try {
            //处理事件（打印）
            LOG.info(prefix + new String(event.getBody()) + suffix);
            //事物提交
            txn.commit();
            status = Status.READY;
        } catch (Exception e) {
            e.printStackTrace();
            //遇到异常 事物回滚
            txn.rollback();
            status = Status.BACKOFF;
        } finally {
            txn.close();
        }
        return status;
    }

    @Override
    public void configure(Context context) {
        //读取配置文件内容，有默认值
        prefix = context.getString("prefix", "hello:");

        //读取配置文件内容，无默认值
        suffix = context.getString("suffix");

    }

    public void tsetGit() {
        System.out.println("此方法与本项目无关");
        System.out.println("用来测试Git");
        System.out.println("以为上次修改没有添加add这是使用add");
        System.out.println("为什么不能clone");
        System.out.println("Clone的是maven项目要配置maven的配置");
        System.out.println("Clone的是maven项目要配置maven的配置");
        System.out.println("Clone的是maven项目要配置maven的配置");
        System.out.println("Clone的是maven项目要配置maven的配置");
        System.out.println("Clone的是maven项目要配置maven的配置");
        System.out.println("Clone的是maven项目要配置maven的配置");
        System.out.println("Clone的是maven项目要配置maven的配置");
        System.out.println("Clone的是maven项目要配置maven的配置");
        System.out.println("Clone的是maven项目要配置maven的配置");
        System.out.println("3333333333333333333333333");
        System.out.println("44444444444444444");

 System.out.println("11111111111111111111111");
        System.out.println("222222222222222222222222");

    }
}

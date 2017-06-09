package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.MappedByteBuffer;

/**
 * 处理client端的请求 Created by wanshao on 2017/5/25.
 */
public class ServerDemoInHandler extends ChannelInboundHandlerAdapter {

    private static final int mapLength = 1024 * 1024 * 1024; //1G
    private static Logger logger = LoggerFactory.getLogger(ServerDemoInHandler.class);
    int i = 0;
    File[] fileList;
    private Channel channel;
    private boolean inited = false;
    private MappedByteBuffer out;
    private int offset = 0; //读到第几个byte

    /**
     * 根据channel
     *
     * @param ctx
     * @return
     */
    public static String getIPString(ChannelHandlerContext ctx) {
        String ipString = "";
        String socketString = ctx.channel().remoteAddress().toString();
        int colonAt = socketString.indexOf(":");
        ipString = socketString.substring(1, colonAt);
        return ipString;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        // 保存channel
//        Server.getMap().put(getIPString(ctx), ctx.channel());
        channel = ctx.channel();

        logger.info("com.alibaba.middleware.race.sync.ServerDemoInHandler.channelRead");
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        // msg中存储的是ByteBuf类型的数据，把数据读取到byte[]中
        result.readBytes(result1);
        String resultStr = new String(result1);
        // 接收并打印客户端的信息
        logger.info("Client said:" + resultStr);


        if (!inited) {
            File file = new File(Constants.DATA_HOME);
            fileList = file.listFiles();
            for (File f :
                    fileList) {
                //输出目录下所有文件名和文件大小
                logger.info("name: {}, size: {} MB", f.getName(), f.length() / 1024. / 1024.);
                FileReader.readOneFile(f.getName(), Server.schemaName, Server.tableName, Server.startPkId, Server.endPkId);
                channel.writeAndFlush(Unpooled.wrappedBuffer(String.format("file %s read done!", f.getName()).getBytes())).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        logger.info("sent read success!");
                    }
                });
            }
            inited = true;
        }

        //发送执行参数
        channel.writeAndFlush(Unpooled.wrappedBuffer(Server.params.getBytes())).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                logger.info("send params success: " + Server.params);
            }
        });

//        int i = 0;
//        while (true) {
////            System.out.println(i);
//            // 向客户端发送消息
//            final byte[] message = getMessage();
//            if (message != null) {
////                Channel channel = Server.getMap().get("127.0.0.1"); //客户端在本地运行所以只取本地
//                ByteBuf byteBuf = Unpooled.wrappedBuffer(message);
//                channel.writeAndFlush(byteBuf).addListener(new ChannelFutureListener() {
//
//                    @Override
//                    public void operationComplete(ChannelFuture future) throws Exception {
//                        logger.info(String.format("sent %s bytes: " + new String(message), message.length));
//                    }
//                });
//            }
//
//            if (i++ > 500) {
//                ctx.close();
//                break;
//            }
//        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    private byte[] getMessage() throws InterruptedException {
        // 模拟下数据生成，每隔5秒产生一条消息
//        Thread.sleep(1000);
//
//        //比赛时在这里产生消息内容
//
//        return "message generated in ServerDemoInHandler. i:" + i++;


        return "".getBytes();

    }
}

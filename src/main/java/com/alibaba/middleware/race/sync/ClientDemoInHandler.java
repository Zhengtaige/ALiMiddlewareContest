package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by wanshao on 2017/5/25.
 */
public class ClientDemoInHandler extends ChannelInboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(ClientDemoInHandler.class);
    //    private int limit = 0;
    FileChannel fileChannel = null;
    private boolean recievedParams = false;
    private String schema;
    private String table;
    private int start;
    private int end;

    // 接收server端的消息，并打印出来
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        logger.info("revived msg.");
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        result.readBytes(result1);
        logger.info("msg length: {}", result1.length);

        try {
            ByteBuffer byteBuffer = ByteBuffer.wrap(result1);
            fileChannel.write(byteBuffer);
        } catch (IOException e) {
            logger.info(e.getMessage());
            e.printStackTrace();
        }

        result.release();
    }

    // 连接成功后，向server发送消息
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("Client({}) channelActive. init file write", ctx.channel().localAddress().toString());
        File file = new File(Constants.RESULT_HOME + "/" + Constants.RESULT_FILE_NAME);
        try {
            fileChannel = new RandomAccessFile(file, "rw")
                    .getChannel();
        } catch (IOException e) {
            logger.info(e.getMessage());
            e.printStackTrace();
        }

        logger.info("send msg to Server");
        String msg = "I am prepared to receive messages";
        ByteBuf encoded = ctx.alloc().buffer(4 * msg.length());
        encoded.writeBytes(msg.getBytes());
        ctx.write(encoded);
        ctx.flush();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        fileChannel.close();
        ctx.close();
    }
}

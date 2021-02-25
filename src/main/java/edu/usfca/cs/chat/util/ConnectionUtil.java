package edu.usfca.cs.chat.util;

import com.google.protobuf.ByteString;
import edu.usfca.cs.chat.ChatMessages;
import edu.usfca.cs.chat.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class ConnectionUtil {

    public static Channel connectController(String hostname,
                                            ChannelInboundHandlerAdapter inboundHandlerAdapter) {

        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(inboundHandlerAdapter);

        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);

        System.out.println("Connecting to Controller " + hostname + ":" + ConfigUtil.controllerPort);
        ChannelFuture cf = bootstrap.connect(hostname, ConfigUtil.controllerPort);
        cf.syncUninterruptibly();
        return cf.channel();
    }

    public static void sendErrorMsgToClient(Channel client, String sourceFilePath, String destFilePath, String errorMsg, String errorType) {
        ChatMessages.Error msg
                = ChatMessages.Error.newBuilder()
                .setErrorReason(errorMsg)
                .setErrorType(errorType)
                .setDestFilePath(destFilePath)
                .setSourcePath(sourceFilePath)
                .build();
        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setError(msg)
                        .build();
        if (client.isActive()) {
            ChannelFuture write = client.writeAndFlush(msgWrapper);
            write.syncUninterruptibly();
        }
    }

    public static void writeToChannel(Channel channel, ChatMessages.ChatMessagesWrapper msgWrapper) {
        ChannelFuture write = channel.writeAndFlush(msgWrapper);
        write.syncUninterruptibly();
    }

    public static ChatMessages.ChatMessagesWrapper formFileContentMsg(String destFilePath, byte[] fileContent, int chunkOrder,
                                                                      long checkSum, int numChunks, String id, String fileType , String fileOps){
        ChatMessages.FileContent msg
                = ChatMessages.FileContent.newBuilder()
                .setFilePath(destFilePath)
                .setFileContent(ByteString.copyFrom(fileContent))
                .setChunkOrder(chunkOrder)
                .setChecksum(checkSum)
                .setNumChunks(numChunks)
                .setParent(id)
                .setFileType(fileType)
                .setFileOps(fileOps)
                .build();
        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setFileContent(msg)
                        .build();
        return msgWrapper;
    }

    public static Channel connectSN (String hostname, int port, ChannelInboundHandlerAdapter inboundHandlerAdapter) {
        try {
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            MessagePipeline pipeline = new MessagePipeline(inboundHandlerAdapter);

            Bootstrap bootstrap = new Bootstrap()
                    .group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(pipeline);

            System.out.println("Connecting to storage node: " + hostname + ":" + port);
            ChannelFuture cf = bootstrap.connect(hostname, port);
            cf.syncUninterruptibly();
            return cf.channel();
        } catch (Exception e) {
            return null;
        }
    }

}

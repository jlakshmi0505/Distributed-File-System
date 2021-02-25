package edu.usfca.cs.chat.util;

import edu.usfca.cs.chat.ChatMessages;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ControllerUtil {
    /**
     * Checks for duplicate username
     */
    public static boolean isDuplicateUsername(String username, ChannelHandlerContext ctx,
                                        final HashMap<String, ChannelHandlerContext> clientToCtx) {
        if (clientToCtx.containsKey(username) && (clientToCtx.get(username) != ctx)) {
            ChannelHandlerContext reg_ctx = clientToCtx.get(username);
            if (reg_ctx.channel().isActive())
                return true;
        }
        return false;
    }

    private static ChatMessages.FileReplicaLocation getReplicaNodeInfo(ChannelHandlerContext ctx,
                                                                       final ConcurrentHashMap<ChannelHandlerContext, HashSet<ChannelHandlerContext>> storageNodeToReplicas,
                                                                       final Map<ChannelHandlerContext, Integer>storageNodeToPort){
        InetSocketAddress addr = null;
        List<ChatMessages.FileReplicaLocation.StorageNodeReplica> storageNodeList = new ArrayList<>();
        HashSet<ChannelHandlerContext> listOfReplicas = storageNodeToReplicas.get(ctx);
        for (ChannelHandlerContext replicaHandler: listOfReplicas) {
            if (storageNodeToPort.containsKey(replicaHandler)) {
                int port = storageNodeToPort.get(replicaHandler);
                addr = (InetSocketAddress) replicaHandler.channel().remoteAddress();
                ChatMessages.FileReplicaLocation.StorageNodeReplica storageNodeReplica
                        = ChatMessages.FileReplicaLocation.StorageNodeReplica.newBuilder()
                        .setUrl(addr.getHostName())
                        .setPort(port)
                        .build();
                storageNodeList.add(storageNodeReplica);
            }
        }
        ChatMessages.FileReplicaLocation fileReplicaLocation = ChatMessages.FileReplicaLocation.newBuilder()
                .addAllStorageNodeReplica(storageNodeList)
                .build();
        return fileReplicaLocation;
    }

    public static void sendActiveNodesInfo(Channel channel,
                                           final Map<ChannelHandlerContext, ChatMessages.Heartbeat> storageNodeToHeartbeat,
                                           final Map<ChannelHandlerContext, String> storageNodeToId) {
        int numActiveNodes = storageNodeToHeartbeat.size();
        List<ChatMessages.ActiveNodesInfo.NodeInfo> activeNodeList = new ArrayList<>();
        for (ChannelHandlerContext ctx : storageNodeToHeartbeat.keySet()) {
            String nodeId = storageNodeToId.get(ctx);
            ChatMessages.ActiveNodesInfo.NodeInfo nodeInfo = ChatMessages.ActiveNodesInfo.NodeInfo.newBuilder()
                    .setId(nodeId)
                    .setInfo(storageNodeToHeartbeat.get(ctx))
                    .build();
            activeNodeList.add(nodeInfo);
        }

        ChatMessages.ActiveNodesInfo activeNodes = ChatMessages.ActiveNodesInfo.newBuilder()
                .setNumActiveNodes(numActiveNodes)
                .addAllNodeInfo(activeNodeList)
                .build();

        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setActiveNodesInfo(activeNodes)
                        .build();

        if (channel.isActive()) {
            ChannelFuture write = channel.writeAndFlush(msgWrapper);
            write.syncUninterruptibly();
        }
    }

    private static ChatMessages.FileWriteRequest createFileWriteRequestMsg(ChatMessages.FileReadRequest msg) {
        String sourceFilePath = msg.getSourceFilePath();
        String destFilePath = msg.getDestFilePath();
        ChatMessages.FileWriteRequest writeMsg
                = ChatMessages.FileWriteRequest.newBuilder()
                .setSourceFilePath(sourceFilePath)
                .setDestFilePath(destFilePath)
                .build();

        return writeMsg;
    }

    private static void sendStorageNodesMetaToClient(Channel channel,
                                              List<ChatMessages.StorageNodeMeta.StorageNode> storageNodeList,
                                              ChatMessages.ChatMessagesWrapper msg) {

        ChatMessages.ChatMessagesWrapper.MsgCase msgCase = msg.getMsgCase();
        ChatMessages.StorageNodeMeta.Builder storageNodeBuilder = ChatMessages.StorageNodeMeta.newBuilder()
                .addAllStorageNodes(storageNodeList);
        switch (msgCase) {
            case FILEREADREQUEST:
                if (msg.getFileReadRequest().getOpType().equals(Constants.READ)) {
                    storageNodeBuilder.setFileReadRequest(msg.getFileReadRequest());
                }
                else if(msg.getFileReadRequest().getOpType().equals(Constants.WRITE)) {
                    storageNodeBuilder.setFileWriteRequest(createFileWriteRequestMsg(msg.getFileReadRequest()));
                    storageNodeBuilder.setMsg(Constants.DUPLICATE);
                }
                break;
            case FILEWRITEREQUEST:
                storageNodeBuilder.setFileWriteRequest(msg.getFileWriteRequest());
                break;
        }
        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setStorageNodeMeta(storageNodeBuilder.build())
                        .build();
        if (channel.isActive()) {
            ChannelFuture write = channel.writeAndFlush(msgWrapper);
            write.syncUninterruptibly();
        }
    }

    private static ChatMessages.StorageNodeMeta.StorageNode formStorageNodeInfoMsg(ChannelHandlerContext handlerContext, Boolean isRead,
                                                                                  final Map<ChannelHandlerContext, ChatMessages.Heartbeat> storageNodeToHeartbeat,
                                                                                  final Map<ChannelHandlerContext, String> storageNodeToId,
                                                                                  final ConcurrentHashMap<ChannelHandlerContext, HashSet<ChannelHandlerContext>> storageNodeToReplicas,
                                                                                  final Map<ChannelHandlerContext, Integer>storageNodeToPort) {
        int port = storageNodeToPort.get(handlerContext);
        String id = storageNodeToId.get(handlerContext);
        InetSocketAddress addr = (InetSocketAddress) handlerContext.channel().remoteAddress();
        ChatMessages.StorageNodeMeta.StorageNode.Builder storageNode = ChatMessages.StorageNodeMeta.StorageNode.newBuilder()
                .setUrl(addr.getHostName())
                .setPort(port)
                .setId(id);
        //send replica info for read request
        if (isRead){
            storageNode.setReplicas(getReplicaNodeInfo(handlerContext, storageNodeToReplicas, storageNodeToPort));
        }
        // send free space info on write request
        else{
            double storageNodeFreeSpace = storageNodeToHeartbeat.get(handlerContext).getFreeSpaceAvailable();
            storageNode.setFreeSpaceAvailable(storageNodeFreeSpace);
        }
        return storageNode.build();
    }

    /**
     * Sends Chat message to the client
     */
    public static void sendChatMessage(String username, String message, Channel channel) {

            ChatMessages.ChatMessage msg
                    = ChatMessages.ChatMessage.newBuilder()
                    .setUsername(username)
                    .setMessageBody(message)
                    .build();

            ChatMessages.ChatMessagesWrapper msgWrapper =
                    ChatMessages.ChatMessagesWrapper.newBuilder()
                            .setChatMessage(msg)
                            .build();

            if (channel.isActive()) {
                ChannelFuture write = channel.writeAndFlush(msgWrapper);
                write.syncUninterruptibly();
            }
        }

    public static void sendStorageNodesMetaMsgToRead(Channel channel, ChatMessages.ChatMessagesWrapper msg,
                                              final Map<ChannelHandlerContext, ChatMessages.Heartbeat> storageNodeToHeartbeat,
                                              final Map<ChannelHandlerContext, String> storageNodeToId,
                                              final ConcurrentHashMap<ChannelHandlerContext, HashSet<ChannelHandlerContext>> storageNodeToReplicas,
                                              final Map<ChannelHandlerContext, Integer>storageNodeToPort,
                                              final Map<ChannelHandlerContext, BloomFilter> storageNodeToBloomFilter){
        String opType = msg.getFileReadRequest().getOpType();
        String destPath = msg.getFileReadRequest().getDestFilePath();
        String sourcePath = msg.getFileReadRequest().getSourceFilePath();
        String filePath = null;
        if (opType.equals(Constants.READ))
            filePath = sourcePath;
        else if (opType.equals(Constants.WRITE))
            filePath = msg.getFileReadRequest().getDestFilePath();

        List<ChatMessages.StorageNodeMeta.StorageNode> storageNodeList = new ArrayList<>();

        for (ChannelHandlerContext handlerContext : storageNodeToBloomFilter.keySet()) {
            if (storageNodeToBloomFilter.get(handlerContext).get(filePath.getBytes())) {
                storageNodeList.add(formStorageNodeInfoMsg(handlerContext, true,
                        storageNodeToHeartbeat, storageNodeToId,
                        storageNodeToReplicas, storageNodeToPort));

            }
        }
        if (storageNodeList.size() == 0){
            String errorType;
            if (opType.equals(Constants.WRITE)){
                errorType = Constants.WRITE;
            } else {
                errorType = Constants.NO_FILE;
            }
            ConnectionUtil.sendErrorMsgToClient(channel, sourcePath, destPath,
                    "File doesn't exist", errorType);
        }else {
            sendStorageNodesMetaToClient(channel, storageNodeList, msg);
        }
    }

    private static List<ChannelHandlerContext> getStorageNodesToWrite(long numChunks, double chunkSize,
                                                               final Map<ChannelHandlerContext, ChatMessages.Heartbeat> storageNodeToHeartbeat) {
        List<ChannelHandlerContext> availCtx = new ArrayList<>();
        /** Filter all available storage nodes with space to store atleast one chunk */
        for (ChannelHandlerContext ctx : storageNodeToHeartbeat.keySet()) {
            double storageNodeFreeSpace = storageNodeToHeartbeat.get(ctx).getFreeSpaceAvailable();
            if (storageNodeFreeSpace >= chunkSize) {
                availCtx.add(ctx);
            }
        }
        /** If available storage nodes greater than total chunks, randomly select k storage nodes,
         * where k = total chunks
         * to store each chunk on one storage node */
        if (availCtx.size() > numChunks) {
            List<ChannelHandlerContext> randomCtx = new ArrayList<>();
            for (int i = 0; i < numChunks; i++) {
                Collections.shuffle(availCtx);
                randomCtx.add(availCtx.remove(0));
            }
            return randomCtx;
        }
        return availCtx;
    }

    public static void sendStorageNodesMetaMsgToWrite(ChatMessages.ChatMessagesWrapper msg, Channel channel,
                                                final Map<ChannelHandlerContext, ChatMessages.Heartbeat> storageNodeToHeartbeat,
                                                final Map<ChannelHandlerContext, String> storageNodeToId,
                                                final ConcurrentHashMap<ChannelHandlerContext, HashSet<ChannelHandlerContext>> storageNodeToReplicas,
                                                final Map<ChannelHandlerContext, Integer>storageNodeToPort) {
        List<ChatMessages.StorageNodeMeta.StorageNode> storageNodeList = new ArrayList<>();
        List<ChannelHandlerContext> ctxList = getStorageNodesToWrite(msg.getFileWriteRequest().getNumChunks(),
                msg.getFileWriteRequest().getChunkSize(), storageNodeToHeartbeat);

        for (ChannelHandlerContext handlerContext : ctxList) {
            storageNodeList.add(formStorageNodeInfoMsg(handlerContext, false,
                                    storageNodeToHeartbeat, storageNodeToId,
                                    storageNodeToReplicas, storageNodeToPort));
        }
        sendStorageNodesMetaToClient(channel, storageNodeList, msg);
    }

    public static void sendReplicaNodeInfoMsgToSN(ChannelHandlerContext ctx,
                                                  final ConcurrentHashMap<ChannelHandlerContext, HashSet<ChannelHandlerContext>> storageNodeToReplicas,
                                                  final Map<ChannelHandlerContext, Integer>storageNodeToPort) {

        ChatMessages.FileReplicaLocation fileReplicaLocation = getReplicaNodeInfo(ctx, storageNodeToReplicas, storageNodeToPort);

        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setFileReplicaLocation(fileReplicaLocation)
                        .build();

        if (ctx.channel().isActive()) {
            ChannelFuture write = ctx.channel().writeAndFlush(msgWrapper);
            write.syncUninterruptibly();
        }
    }

    public static void sendFileSystemTree(Channel channel, String fileSystemTree) {
        ChatMessages.FileTreeInfo msg
                = ChatMessages.FileTreeInfo.newBuilder()
                .setFileTree(fileSystemTree)
                .build();

        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setFileTreeInfo(msg)
                        .build();

        if (channel.isActive()) {
            ChannelFuture write = channel.writeAndFlush(msgWrapper);
            write.syncUninterruptibly();
        }
    }
}

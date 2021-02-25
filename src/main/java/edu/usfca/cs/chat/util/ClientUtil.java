package edu.usfca.cs.chat.util;

import edu.usfca.cs.chat.ChatMessages;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientUtil {

    public static ChatMessages.ChatMessagesWrapper sendRegistration(String username) {
        ChatMessages.ClientRegistration reg
                = ChatMessages.ClientRegistration.newBuilder()
                .setUsername(username)
                .build();

        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setRegistration(reg)
                        .build();

        return msgWrapper;
    }

    public static String clientUsageInfo() {
        StringBuilder stringBuilder = new StringBuilder(System.lineSeparator());
        stringBuilder.append("Client supports the following commands:");
        stringBuilder.append(System.lineSeparator());
        stringBuilder.append("1. Writing a file to DFS: write <localSourceFilePath> <dfsFilePath>");
        stringBuilder.append(System.lineSeparator());
        stringBuilder.append("2. Reading a file from DFS: read <dfsFilePath> <localDestinationPath>");
        stringBuilder.append(System.lineSeparator());
        stringBuilder.append("3. DFS Storage Nodes info: info");
        stringBuilder.append(System.lineSeparator());
        stringBuilder.append("4. File System Tree: ls");
        stringBuilder.append(System.lineSeparator());
        return stringBuilder.toString();
    }

    public static void disconnectSN(Channel snChannel) {
        snChannel.disconnect();
    }

    public static String printNodeInfo(ChatMessages.ActiveNodesInfo activeNodesInfo) {
        StringBuilder stringBuilder = new StringBuilder(System.lineSeparator());
        stringBuilder.append("Number of active nodes = ");
        stringBuilder.append(activeNodesInfo.getNumActiveNodes());
        stringBuilder.append(System.lineSeparator());
        for (ChatMessages.ActiveNodesInfo.NodeInfo nodeInfo : activeNodesInfo.getNodeInfoList()) {
            stringBuilder.append("Node: ");
            stringBuilder.append(nodeInfo.getId());
            stringBuilder.append(" available space = ");
            stringBuilder.append(nodeInfo.getInfo().getFreeSpaceAvailable());
            stringBuilder.append(" and request processed = ");
            stringBuilder.append(nodeInfo.getInfo().getRequestsProcessedCount());
            stringBuilder.append(System.lineSeparator());
        }
        return stringBuilder.toString();
    }

    public static void sendFileWriteRequest(String sourceFilePath, String destFilePath, Channel controllerChannel) {
        File file = new File(sourceFilePath);
        int totalChunks = (int) Math.ceil((float) file.length() / Constants.CHUNK_SIZE);
        sendFileWriteRequestMsg(controllerChannel, sourceFilePath, destFilePath, file.length(), Constants.CHUNK_SIZE, totalChunks);
    }

    private static void sendFileWriteRequestMsg(Channel controllerChannel, String sourceFilePath, String destFilePath, double fileSize,
                                                double chunkSize, long totalChunks) {
        ChatMessages.FileWriteRequest msg
                = ChatMessages.FileWriteRequest.newBuilder()
                .setSourceFilePath(sourceFilePath)
                .setDestFilePath(destFilePath)
                .setFileSize(fileSize)
                .setChunkSize(chunkSize / 1024 / 1024)
                .setNumChunks(totalChunks)
                .build();

        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setFileWriteRequest(msg)
                        .build();

        ConnectionUtil.writeToChannel(controllerChannel, msgWrapper);
    }

    public static void breakFileToChunks(ChatMessages.StorageNodeMeta msg, ChannelInboundHandlerAdapter inboundHandler) {
        String sourceFilePath = msg.getFileWriteRequest().getSourceFilePath();
        String destFilePath = msg.getFileWriteRequest().getDestFilePath();
        List<ChatMessages.StorageNodeMeta.StorageNode> storageNodeList = msg.getStorageNodesList();

        File file = new File(sourceFilePath);
        int numChunks = (int) Math.ceil((float) file.length() / Constants.CHUNK_SIZE);
        try {
            InputStream inputStream = new FileInputStream(file);
            writeFileToStorageNode(inputStream, destFilePath, numChunks, storageNodeList,inboundHandler);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeFileToStorageNode(InputStream inputStream, String destFilePath, int numChunks, List<
            ChatMessages.StorageNodeMeta.StorageNode> storageNodes, ChannelInboundHandlerAdapter inboundHandler) {
        Map<String, Double> snFreeSpace = new HashMap<>();
        String snIP = null;
        int count = 0;
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        int chunkOrder = 0;
        String hostname = null;
        int port = 0;
        int nRead = 0;
        int numSN = 0;

        System.out.println("Storage nodes fetched to write: " + storageNodes.size());

        while (true) {
            byte[] buffer = new byte[Constants.CHUNK_SIZE];
            try {
                if ((nRead = bufferedInputStream.read(buffer)) == -1) break;
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (numSN >= storageNodes.size())
                numSN = 0;

            count = 0;
            /** Find the storage node which has free space to store a chunk */
            while (count < storageNodes.size()) {
                hostname = storageNodes.get(numSN).getUrl();
                port = storageNodes.get(numSN).getPort();
                snIP = hostname + port;
                if (snFreeSpace.containsKey(snIP) && snFreeSpace.get(snIP) < Constants.CHUNK_SIZE)
                    if (numSN >= storageNodes.size()) numSN = 0;
                    else numSN += 1;
                else
                    break;
                count += 1;
            }
            if (count >= storageNodes.size()) {
                System.out.println("*** Not enough space to store the complete file ***");
                break;
            }

            Channel snChannel = ConnectionUtil.connectSN(hostname, port, inboundHandler);
            if (snChannel.isActive() == false) {
                System.out.println("*** Write failed!!!! Please try again ***");
                break;
            }
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            byteArrayOutputStream.write(buffer, 0, nRead);
            buffer = Arrays.copyOfRange(buffer, 0, nRead);

            long checkSum = FileUtil.getChecksum(buffer);

            ChatMessages.ChatMessagesWrapper msgWrapper = ConnectionUtil.formFileContentMsg(destFilePath, buffer,
                    chunkOrder, checkSum,
                    numChunks, "",
                    Constants.PRIMARY,"");

            ConnectionUtil.writeToChannel(snChannel, msgWrapper);

            System.out.println("written chunk: " + chunkOrder + "/" + (numChunks - 1));

            if (snFreeSpace.containsKey(snIP)) {
                double freeSpace = storageNodes.get(numSN).getFreeSpaceAvailable() - (snFreeSpace.get(snIP) + Constants.CHUNK_SIZE / 1024 / 1024);
                snFreeSpace.put(snIP, freeSpace);
            }
            chunkOrder += 1;
            numSN += 1;
        }

        if (count < storageNodes.size())
            System.out.println("====Completed writing file to DFS successfully!!!!====");

    }

    public static void sendFileReadRequest(Channel controllerChannel, String line, String opType) {
        String[] inputArgs = line.split(" ", 3);
        String sourceFile = inputArgs[1];
        String destFile = inputArgs[2];

        ChatMessages.FileReadRequest msg
                = ChatMessages.FileReadRequest
                .newBuilder()
                .setDestFilePath(destFile)
                .setSourceFilePath(sourceFile)
                .setOpType(opType)
                .build();

        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setFileReadRequest(msg)
                        .build();

        ConnectionUtil.writeToChannel(controllerChannel, msgWrapper);

    }
    public static void sendReadFileMessageToStorageNode(Channel snChannel, String sourceFile, String destFile, int numStorageNodes) {
        ChatMessages.FileRead msg
                = ChatMessages.FileRead
                .newBuilder()
                .setSourcePath(sourceFile)
                .setDestPath(destFile)
                .setTotalStorageNodes(numStorageNodes)
                .build();
        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setFileRead(msg)
                        .build();

        ConnectionUtil.writeToChannel(snChannel, msgWrapper);
    }

    public static void sendDeleteChunksReqMsgToSN(Channel snChannel, String filePath, String sourcePath) {
        ChatMessages.DeleteChunksReq msg
                = ChatMessages.DeleteChunksReq
                .newBuilder()
                .setFilePath(filePath)
                .setSourcePath(sourcePath)
                .build();
        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setDeleteChunksReq(msg)
                        .build();

        ConnectionUtil.writeToChannel(snChannel, msgWrapper);
    }

    public static void sendDfsInfoRequest(Channel controllerChannel, String infoType) {
        ChatMessages.RequestInfo msg = ChatMessages.RequestInfo
                .newBuilder()
                .setInfoType(infoType)
                .build();
        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setRequestInfo(msg)
                        .build();

        ConnectionUtil.writeToChannel(controllerChannel, msgWrapper);
    }
}

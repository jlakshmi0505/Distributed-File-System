package edu.usfca.cs.chat.util;

import com.google.protobuf.ByteString;
import edu.usfca.cs.chat.ChatMessages;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;

import java.io.File;
import java.util.StringJoiner;


public class StorageNodeUtil {
    // Registration msg from SN to Controller
    public static void sendSNRegistration(Channel controllerChannel, String id, int listenPort) {
        ChatMessages.SNRegistration reg
                = ChatMessages.SNRegistration.newBuilder()
                .setPort(listenPort)
                .setId(id)
                .build();

        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setStorageNodeRegistration(reg)
                        .build();

        ConnectionUtil.writeToChannel(controllerChannel, msgWrapper);
    }

    /** Send deleted chunks response to Client on file rewrite */
    public static void sendDeleteChunksResp (ChatMessages.DeleteChunksReq msg, Channel channel, int success) {
        ChatMessages.DeleteChunksResp resp = ChatMessages.DeleteChunksResp.newBuilder()
                .setFilePath(msg.getFilePath())
                .setSourcePath(msg.getSourcePath())
                .setSuccess(success)
                .build();
        ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
                .setDeleteChunksResp(resp)
                .build();

        if (channel.isActive()) {
            ChannelFuture write = channel.writeAndFlush(msgWrapper);
            write.syncUninterruptibly();
        }
    }

    /** Delete existing primary file on file rewrite */
    public static int deleteExistingPrimaryDirectory (String filePath, String storageDirectory) {
        File file = new File(storageDirectory + "/" + filePath);
        if (file.exists()) {
            FileUtil.cleanDirectory(file, true);
            return 1;
        }
        return 0;
    }


    // write the meta and file chunk to the disk on write
    public static void writeFileChunkAndMetaToDisk(ChatMessages.FileContent fileWriteMessage, String storageDirectory) {
        long inputCS = fileWriteMessage.getChecksum();
        ByteString fileContent = fileWriteMessage.getFileContent();
        byte[] fileContentBytes = fileContent.toByteArray();
        int chunkOrder = fileWriteMessage.getChunkOrder();
        String destFilePath = fileWriteMessage.getFilePath();
        String fileType = fileWriteMessage.getFileType();
        int numChunks = fileWriteMessage.getNumChunks();
        String parent = fileWriteMessage.getParent();
        if (!parent.equals("") && (!destFilePath.contains(parent) && fileType.equals(Constants.SECONDARY))){
            destFilePath = FileUtil.getFileName(parent, destFilePath);
        }
        String filePathWithChunk = FileUtil.getFileName(storageDirectory, destFilePath, String.valueOf(chunkOrder));
        String metaFilePath = FileUtil.getMetaFileName(filePathWithChunk);
        FileUtil.writeFileContent(filePathWithChunk, fileContentBytes);
        FileUtil.writeMetaJsonToFile(metaFilePath, destFilePath, inputCS, fileType, numChunks, chunkOrder, parent);
    }

    // send  acknowledgement to the controller on write
    public static void sendWriteAckMessageToController(Channel controllerChannel, String filePath) {
        ChatMessages.FileWriteAck msg
                = ChatMessages.FileWriteAck.newBuilder()
                .setFilePath(filePath)
                .build();

        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setFileWriteAck(msg)
                        .build();

        ConnectionUtil.writeToChannel(controllerChannel, msgWrapper);
    }

   // Send file read request to replica node on checksum failure
    public static void sendFileReplicaReadMsg(Channel replica, String id, String sourcePath, String destPath, long checkSum, int chunkOrder, int numChunk) {
        // adding the node id to the source filePath
        sourcePath = FileUtil.getFileName(id, sourcePath);
        ChatMessages.FileReplicaRead msg
                = ChatMessages.FileReplicaRead.newBuilder()
                .setSourcePath(sourcePath)
                .setDestPath(destPath)
                .setChecksum(checkSum)
                .setChunkOrder(chunkOrder)
                .setNumChunks(numChunk)
                .build();
        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setFileReplicaReadRequest(msg)
                        .build();
        ConnectionUtil.writeToChannel(replica, msgWrapper);
    }

    // send file doesn't exits info to client on read
    public static void sendNoFileContentMsgToClient(ChannelHandlerContext ctx, String path, int totalStorageNodes) {
        ChatMessages.NoFileContent msg
                = ChatMessages.NoFileContent.newBuilder()
                .setDestPath(path)
                .setTotalStorageNodes(totalStorageNodes)
                .build();
        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setNoFileContent(msg)
                        .build();
        Channel clientChannel = ctx.channel();
        clientChannel.writeAndFlush(msgWrapper);
    }

    //delete replica folder or specific file in folder if filepath is mentioned
    public static void deleteFolderInReplica(ChatMessages.DeleteReplica deleteReplica, String storageDirectory){
        String sourcePath = FileUtil.getFileName(storageDirectory, deleteReplica.getParentId());
        StringJoiner stringJoiner = new StringJoiner("/");
        stringJoiner.add(sourcePath);
        if (deleteReplica.getFilePath() != null) {
            stringJoiner.add(deleteReplica.getFilePath());
        }
        sourcePath = stringJoiner.toString();
        FileUtil.cleanDirectory(sourcePath, true);
        System.out.println("Deleted: " + sourcePath);
    }

    // send the replica file content to the primary node from the replica node
    public static void sendReplicaFileContent(ChannelHandlerContext ctx, ChatMessages.FileReplicaRead fileReplicaRead, byte[] fileContent) {
        ChatMessages.ReplicaFileContent msg
                = ChatMessages.ReplicaFileContent.newBuilder()
                .setSourcePath(fileReplicaRead.getSourcePath())
                .setDestPath(fileReplicaRead.getDestPath())
                .setFileContent(ByteString.copyFrom(fileContent))
                .setChunkOrder(fileReplicaRead.getChunkOrder())
                .setChecksum(fileReplicaRead.getChecksum())
                .setNumChunks(fileReplicaRead.getNumChunks())
                .build();
        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setReplicaFileContent(msg)
                        .build();
        Channel clientChannel = ctx.channel();
        clientChannel.writeAndFlush(msgWrapper);
    }
}

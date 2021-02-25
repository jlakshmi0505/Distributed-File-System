package edu.usfca.cs.chat;

import com.google.protobuf.ByteString;
import edu.usfca.cs.chat.net.ServerMessageRouter;
import edu.usfca.cs.chat.util.ConnectionUtil;
import edu.usfca.cs.chat.util.Constants;
import edu.usfca.cs.chat.util.FileUtil;
import edu.usfca.cs.chat.util.StorageNodeUtil;
import io.netty.channel.*;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

@ChannelHandler.Sharable
public class StorageNode
        extends SimpleChannelInboundHandler<ChatMessages.ChatMessagesWrapper> {

    ServerMessageRouter messageRouter;
    HashMap<String, String> replicaNodesInfo = new HashMap<>();

    private final String storageDirectory;
    private final String hostname;

    private final int listenPort;
    private final String id;

    private static int requestsProcessedCount;
    private static ScheduledExecutorService heartbeatService;
    private ExecutorService diskAccessExecutors = Executors.newCachedThreadPool();
    private ExecutorService replicaSendExecutors = Executors.newCachedThreadPool();
    private ExecutorService replicaAccessExecutors = Executors.newCachedThreadPool();
    private ExecutorService copyReplicaAccessExecutors = Executors.newCachedThreadPool();
    private ExecutorService deleteReplicaAccessExecutors = Executors.newCachedThreadPool();

    private AtomicInteger deletedReplicaReq = new AtomicInteger(0);
    private AtomicInteger deletedReplicaResp = new AtomicInteger(0);

    private static Channel controllerChannel;
    private ConcurrentMap<String, ChannelHandlerContext> fileChunkToClient = new ConcurrentHashMap<>();

    private final static Logger log = Logger.getLogger("StorageNode");

    public StorageNode(String hostname, int listenPort, String directory) {
        this.hostname = hostname;
        this.storageDirectory = directory;
        this.listenPort = listenPort;
        this.id = UUID.randomUUID().toString();
        // create the storage directory if not present
        FileUtil.makeDirectory(this.storageDirectory);
        FileUtil.cleanDirectory(this.storageDirectory, false);
    }

    public void start(int port)
            throws IOException {
        messageRouter = new ServerMessageRouter(this);
        messageRouter.listen(port);
        System.out.println("Listening on port " + port + "...");
    }

    public static void sendHeartbeat(String storageDirectory) {
        if (!controllerChannel.isActive()) {
            heartbeatService.shutdown();
            return;
        }

        long freeSpace = new File(storageDirectory).getFreeSpace();

        ChatMessages.Heartbeat hb
                = ChatMessages.Heartbeat.newBuilder()
                .setFreeSpaceAvailable(freeSpace / 1024 / 1024)
                .setRequestsProcessedCount(requestsProcessedCount)
                .build();

        ChatMessages.ChatMessagesWrapper msgWrapper =
                ChatMessages.ChatMessagesWrapper.newBuilder()
                        .setHeartbeat(hb)
                        .build();

        ConnectionUtil.writeToChannel(controllerChannel, msgWrapper);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Usage: StorageNode <hostname> <listening_port> <directory_path>");
            System.exit(1);
        }

        final StorageNode sn = new StorageNode(args[0], Integer.parseInt(args[1]), args[2]);

        sn.start(sn.listenPort);
        sn.controllerChannel = ConnectionUtil.connectController(sn.hostname, sn);
        StorageNodeUtil.sendSNRegistration(sn.controllerChannel, sn.id, sn.listenPort);
        //send heartbeats to server every 5 seconds
        Runnable runnable = () -> sendHeartbeat(sn.storageDirectory);

        heartbeatService = Executors.newSingleThreadScheduledExecutor();
        heartbeatService.scheduleAtFixedRate(runnable, 5, 5, TimeUnit.SECONDS);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr
                = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr
                = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection lost: " + addr);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
            throws Exception {
        /* Writable status of the channel changed */
    }

    @Override
    public void channelRead0(
            ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) {

        ChatMessages.ChatMessagesWrapper.MsgCase msgCase = msg.getMsgCase();

        switch (msgCase) {
            case FILECONTENT: {
                ChatMessages.FileContent fileMessage = msg.getFileContent();
                requestsProcessedCount += 1;
                if (fileMessage.getFileType().equals(Constants.PRIMARY)) {
                    diskAccessExecutors.submit(new WriteToLocalDisk(fileMessage, true));
                } else {
                    replicaAccessExecutors.submit(new WriteToLocalDisk(fileMessage, false));
                }
                ctx.close();
            }
            break;
            case FILEREAD: {
                requestsProcessedCount += 1;
                ChatMessages.FileRead fileReadMessage = msg.getFileRead();
                diskAccessExecutors.submit(new ReadFileFromLocalDisk(fileReadMessage, ctx));
            }
            break;
            case FILEREPLICALOCATION: {
                ChatMessages.FileReplicaLocation fileReplicaLocation = msg.getFileReplicaLocation();
                updateReplicaList(fileReplicaLocation);
            }
            break;
            case FILEREPLICAREADREQUEST: {
                requestsProcessedCount += 1;
                ChatMessages.FileReplicaRead fileReplicaRead = msg.getFileReplicaReadRequest();
                diskAccessExecutors.submit(new ReadReplicaFileFromLocalDisk(fileReplicaRead, ctx));
            }
            break;
            case REPLICAFILECONTENT: {
                ChatMessages.ReplicaFileContent replicaFileContent = msg.getReplicaFileContent();
                diskAccessExecutors.submit(new ReWriteCorruptedFileToLocalDisk(replicaFileContent));
                ctx.close();
            }
            break;
            case DELETECHUNKSREQ: {
                String filePath = msg.getDeleteChunksReq().getFilePath();
                int success = StorageNodeUtil.deleteExistingPrimaryDirectory(filePath, storageDirectory);
                sendDeleteReplicaMsg(filePath);
                StorageNodeUtil.sendDeleteChunksResp(msg.getDeleteChunksReq(), ctx.channel(), success);
                ctx.close();
            }
            break;
            case COPYREPLICA: {
                requestsProcessedCount += 1;
                ChatMessages.CopyReplica msgCopyReplica = msg.getCopyReplica();
                copyReplicaAccessExecutors.submit(new ReadFromReplica(msgCopyReplica, ctx));
            }
            break;
            case DELETEREPLICA: {
                ChatMessages.DeleteReplica msgDeleteReplica = msg.getDeleteReplica();
                deleteReplicaAccessExecutors.submit(new DeleteReplicaFolder(msgDeleteReplica, ctx));
            }
            break;
        }
    }

    /** Send delete secondary copies request to storage node replicas */
    private void sendDeleteReplicaMsg(String filePath) {
        for (String replicaName: replicaNodesInfo.keySet()) {
            Channel replicaChannel = connectToReplicaSN(replicaName);
            ChatMessages.DeleteReplica replica = ChatMessages.DeleteReplica.newBuilder()
                    .setFilePath(filePath)
                    .setParentId(id)
                    .build();
            ChatMessages.ChatMessagesWrapper msgWrapper = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setDeleteReplica(replica)
                    .build();

            ConnectionUtil.writeToChannel(replicaChannel, msgWrapper);
        }
    }

    private void updateReplicaList(ChatMessages.FileReplicaLocation fileReplicaLocation) {
        List<ChatMessages.FileReplicaLocation.StorageNodeReplica> replicas = fileReplicaLocation.getStorageNodeReplicaList();
        if (replicas.size() == 1) {
            replicaNodesInfo.remove("Replica-2");
        }
        int i = 1;
        for (ChatMessages.FileReplicaLocation.StorageNodeReplica r : replicas) {
            String key = "Replica-" + i;
            int port = r.getPort();
            String hostName = r.getUrl();
            String s = hostName + "," + port;
            replicaNodesInfo.put(key, s);
            i++;
        }
        log.log(Level.INFO, "My replicas are");
        for (String s : replicaNodesInfo.keySet()) {
            System.out.println(s + replicaNodesInfo.get(s));
        }
    }

    private Channel connectToReplicaSN(String replica) {
        log.log(Level.INFO, "Connecting to {0} to write the replica files in directory {1}",
                new Object[]{replica, id});
        String replicaNodeInfo = replicaNodesInfo.get(replica);
        String[] inputArgs = replicaNodeInfo.split(",", 2);
        // return connectToReplica(inputArgs[0], Integer.parseInt(inputArgs[1]));
        return ConnectionUtil.connectSN(inputArgs[0], Integer.parseInt(inputArgs[1]), this);

    }

    // send the file chunks to the replicas on write
    private void sendChunksToReplica(ChatMessages.FileContent fileWriteMessage) {
        ArrayList<Channel> replicaChannels = new ArrayList<>();

        for (String replica : replicaNodesInfo.keySet()) {
            Channel replicaChannel = connectToReplicaSN(replica);
            replicaChannels.add(replicaChannel);
        }

        ByteString fileContent = fileWriteMessage.getFileContent();

        ChatMessages.ChatMessagesWrapper msgWrapper = ConnectionUtil.formFileContentMsg(fileWriteMessage.getFilePath(),
                fileContent.toByteArray(), fileWriteMessage.getChunkOrder(),
                fileWriteMessage.getChecksum(), fileWriteMessage.getNumChunks(),
                id, Constants.SECONDARY, "");

        for (Channel replicaChannel : replicaChannels) {
            if (replicaChannel != null) {
                ConnectionUtil.writeToChannel(replicaChannel, msgWrapper);
            }
        }
    }

    // send the file content to client on read
    private void sendFileToClient(ChannelHandlerContext ctx, byte[] fileContent,
                                  String sourcePath, String destPath,
                                  long checkSum, int chunkOrder,
                                  int numChunk) {
        if (FileUtil.verifyChecksum(fileContent, checkSum)) {

            ChatMessages.ChatMessagesWrapper msgWrapper = ConnectionUtil.formFileContentMsg(destPath, fileContent, chunkOrder,
                    checkSum, numChunk, "", "", "");

            Channel clientChannel = ctx.channel();
            clientChannel.writeAndFlush(msgWrapper);
        } else {
            // store the client ctx for the corrupted file before getting the file from the
            // remove the storage directory from the source filePath
            sourcePath = sourcePath.replace(storageDirectory, "");
            log.log(Level.INFO, "checksum verification failed for file {0}", sourcePath);
            if (replicaNodesInfo.size() > 0) {
                fileChunkToClient.put(sourcePath, ctx);
                Channel replicaChannel = null;

                for (String replica : replicaNodesInfo.keySet()) {
                    replicaChannel = connectToReplicaSN(replica);
                    if (replicaChannel != null) {
                        break;
                    }
                }

                if (replicaChannel == null) {
                    log.log(Level.INFO, "Replicas couldn't be connected");
                    ConnectionUtil.sendErrorMsgToClient(ctx.channel(), sourcePath, destPath,
                            "Read Failure due to corrupted file",
                            Constants.CHECK_SUM_ERROR);
                } else {
                    // read the file from the replica node
                    log.log(Level.INFO, "Reading replica file for {0} from replica {1} in directory {2}",
                            new Object[]{sourcePath, replicaChannel.remoteAddress(), id});
                    StorageNodeUtil.sendFileReplicaReadMsg(replicaChannel, id, sourcePath, destPath,
                            checkSum, chunkOrder, numChunk);
                }
            } else {
                log.log(Level.INFO, "No replica exists");
                ConnectionUtil.sendErrorMsgToClient(ctx.channel(), sourcePath, destPath,
                        "Read Failure due to corrupted file",
                        Constants.CHECK_SUM_ERROR);
            }
        }
    }


    private void sendFileToReplica(ChannelHandlerContext ctx, byte[] fileContent, long checkSum,
                                   int chunkOrder, int numChunk, ChatMessages.CopyReplica copyReplica, String destPath) {
        //    Channel replicaCtx = connectToReplica(copyReplica.getHostName(),copyReplica.getPort());
        Channel replicaCtx = ConnectionUtil.connectSN(copyReplica.getHostName(), copyReplica.getPort(), this);
        String parentId = "";

        if (copyReplica.getFileType().equals(Constants.SECONDARY)) {
            parentId = copyReplica.getParentId();
        }

        ChatMessages.ChatMessagesWrapper msgWrapper = ConnectionUtil.formFileContentMsg(destPath, fileContent,
                chunkOrder, checkSum, numChunk,
                parentId, copyReplica.getFileType(), Constants.COPY);

        ChannelFuture write = replicaCtx.writeAndFlush(msgWrapper);
        write.syncUninterruptibly();
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }

    // read from local disk
    private class ReadFileFromLocalDisk implements Runnable {
        private ChatMessages.FileRead fileReadMessage;
        private ChannelHandlerContext ctx;

        public ReadFileFromLocalDisk(ChatMessages.FileRead message, ChannelHandlerContext ctx) {
            this.fileReadMessage = message;
            this.ctx = ctx;
        }

        @Override
        public void run() {
            String sourcePath = FileUtil.getFileName(storageDirectory, fileReadMessage.getSourcePath());
            String destPath = fileReadMessage.getDestPath();

            List<JSONObject> primaryMetas = FileUtil.getMetaFileChunks(sourcePath);
            if (primaryMetas.size() > 0) {
                ExecutorService readExecutors = Executors.newFixedThreadPool(primaryMetas.size());
                for (JSONObject primaryMeta : primaryMetas) {
                    String chunkFilePath = (String) primaryMeta.get(Constants.CHUNK_FILE_PATH);
                    int chunkOrder = ((Long) primaryMeta.get(Constants.CHUNK_ORDER)).intValue();
                    int numChunk = ((Long) primaryMeta.get(Constants.NUM_CHUNKS)).intValue();
                    long checksum = (Long) primaryMeta.get(Constants.CHECKSUM);
                    readExecutors.submit(new SendFileContent(ctx, chunkFilePath, destPath, checksum, chunkOrder, numChunk));
                }
                readExecutors.shutdown();
            } else {
                log.log(Level.INFO, "file {0} doesnot exist in the SN(False positive)",
                        fileReadMessage.getSourcePath());
                StorageNodeUtil.sendNoFileContentMsgToClient(ctx, destPath, fileReadMessage.getTotalStorageNodes());
            }
        }
    }

    // send the file content of each file in the SN to the client
    private class SendFileContent implements Runnable {
        private ChannelHandlerContext client;
        private long checksum;
        private String destFilePath;
        private String sourceFilePath;
        private int chunkOrder;
        private int numChunk;

        public SendFileContent(ChannelHandlerContext ctx,
                               String sourceFilePath, String destFilePath,
                               long checksum, int chunkOrder, int numChunk) {
            this.client = ctx;
            this.checksum = checksum;
            this.destFilePath = destFilePath;
            this.sourceFilePath = sourceFilePath;
            this.chunkOrder = chunkOrder;
            this.numChunk = numChunk;
        }

        @Override
        public void run() {
            byte[] fileContent = FileUtil.readFileContent(this.sourceFilePath);
            sendFileToClient(this.client, fileContent,
                    this.sourceFilePath, this.destFilePath,
                    this.checksum, this.chunkOrder, this.numChunk);
        }
    }

    // write file chunk to local disk
    private class WriteToLocalDisk implements Runnable {
        private ChatMessages.FileContent fileWriteMessage;
        private Boolean isPrimaryWrite;

        public WriteToLocalDisk(ChatMessages.FileContent message, boolean isPrimaryWrite) {
            this.fileWriteMessage = message;
            this.isPrimaryWrite = isPrimaryWrite;
        }

        @Override
        public void run() {
            String destFilePath =  FileUtil.sanitizeFilePath(fileWriteMessage.getFilePath());
            String fileType = fileWriteMessage.getFileType();
            if (fileWriteMessage.getFileOps() != "" && fileWriteMessage.getFileOps().equals(Constants.COPY)) {
                System.out.println("Copying files ...");
            }
            StorageNodeUtil.writeFileChunkAndMetaToDisk(fileWriteMessage, storageDirectory);
            if (replicaNodesInfo.size() > 0 && isPrimaryWrite) {
                replicaSendExecutors.submit(new SendReplicaInfo(fileWriteMessage));
            }
            if (isPrimaryWrite && fileType.equals(Constants.PRIMARY)) {
                destFilePath = FileUtil.sanitizeFilePath(destFilePath);
                StorageNodeUtil.sendWriteAckMessageToController(controllerChannel, destFilePath);
            }
        }
    }

    // send the file chunks to replica nodes
    private class SendReplicaInfo implements Runnable {
        private ChatMessages.FileContent sendReplicaInfo;

        public SendReplicaInfo(ChatMessages.FileContent message) {
            this.sendReplicaInfo = message;
        }

        @Override
        public void run() {
            sendChunksToReplica(sendReplicaInfo);
        }
    }

    // Read the replica file in the secondary node and send it to the primary node on checksum error
    private class ReadReplicaFileFromLocalDisk implements Runnable {
        private ChatMessages.FileReplicaRead fileReplicaRead;
        private ChannelHandlerContext ctx;

        public ReadReplicaFileFromLocalDisk(ChatMessages.FileReplicaRead message, ChannelHandlerContext ctx) {
            this.fileReplicaRead = message;
            this.ctx = ctx;
        }

        @Override
        public void run() {
            log.log(Level.INFO, "Reading replica file :{0}", fileReplicaRead.getSourcePath());
            String sourcePath = FileUtil.getFileName(storageDirectory, fileReplicaRead.getSourcePath());
            byte[] fileContent = FileUtil.readFileContent(sourcePath);

            StorageNodeUtil.sendReplicaFileContent(ctx, fileReplicaRead, fileContent);
        }
    }

    // rewrite the corrupted file in SN from the replica file and send teh file to client
    private class ReWriteCorruptedFileToLocalDisk implements Runnable {
        private ChatMessages.ReplicaFileContent fileContent;

        public ReWriteCorruptedFileToLocalDisk(ChatMessages.ReplicaFileContent message) {
            this.fileContent = message;
        }

        @Override
        public void run() {
            // removing the id from the source filePath
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(id);
            stringBuilder.append('/');
            String id_path = stringBuilder.toString();
            String sourcePath = fileContent.getSourcePath().replace(id_path, "");
            ChannelHandlerContext client = fileChunkToClient.get(sourcePath);
            fileChunkToClient.remove(sourcePath);
            log.log(Level.INFO, "Rewrite the file : {0}", sourcePath);
            // adding the storage directory to the source filePath
            sourcePath = FileUtil.getFileName(storageDirectory, sourcePath);
            // rewriting the filecontent in the primary node
            byte[] fileByteArrayContent = fileContent.getFileContent().toByteArray();
            FileUtil.writeFileContent(sourcePath, fileByteArrayContent);
            sendFileToClient(client, fileByteArrayContent,
                    sourcePath, fileContent.getDestPath(),
                    fileContent.getChecksum(), fileContent.getChunkOrder(),
                    fileContent.getNumChunks());
        }
    }


    private class ReadFromReplica implements Runnable {
        private ChatMessages.CopyReplica copyReplica;
        private ChannelHandlerContext ctx;

        public ReadFromReplica(ChatMessages.CopyReplica copyReplica, ChannelHandlerContext ctx) {
            this.copyReplica = copyReplica;
            this.ctx = ctx;
        }

        @Override
        public void run() {
            String sourcePath = FileUtil.getFileName(storageDirectory, copyReplica.getParentId());
            List<JSONObject> primaryMetas = FileUtil.getAllMetaFiles(sourcePath);
            ExecutorService readReplicaExecutors = Executors.newFixedThreadPool(primaryMetas.size());
            String destPath = null;
            System.out.println("sending copy request");
            for (JSONObject primaryMeta : primaryMetas) {
                String chunkFilePath = (String) primaryMeta.get(Constants.CHUNK_FILE_PATH);
                int chunkOrder = ((Long) primaryMeta.get(Constants.CHUNK_ORDER)).intValue();
                int numChunk = ((Long) primaryMeta.get(Constants.NUM_CHUNKS)).intValue();
                long checksum = (Long) primaryMeta.get(Constants.CHECKSUM);
                if (copyReplica.getFileType().equals(Constants.PRIMARY)) {
                    destPath = (String) primaryMeta.get(Constants.FILE_PATH);
                    if (destPath != null && (!destPath.isEmpty())) {
                        destPath = destPath.split("/", 2)[1];
                        destPath = FileUtil.sanitizeFilePath(destPath);
                    }
                } else {
                    destPath = (String) primaryMeta.get(Constants.FILE_PATH);
                }
                readReplicaExecutors.submit(new SendFileToReplica(ctx, checksum, chunkFilePath, chunkOrder, numChunk, copyReplica, destPath));
            }
            readReplicaExecutors.shutdown();
        }
    }

    private class SendFileToReplica implements Runnable {
        private ChannelHandlerContext client;
        private long checksum;
        private String sourceFilePath;
        private int chunkOrder;
        private int numChunk;
        private ChatMessages.CopyReplica copyReplica;
        private String destPath;

        public SendFileToReplica(ChannelHandlerContext ctx, long checksum,
                                 String sourcefilePath,
                                 int chunkOrder, int numChunk, ChatMessages.CopyReplica copyReplica, String destPath) {
            this.client = ctx;
            this.checksum = checksum;
            this.sourceFilePath = sourcefilePath;
            this.chunkOrder = chunkOrder;
            this.numChunk = numChunk;
            this.copyReplica = copyReplica;
            this.destPath = destPath;
        }

        @Override
        public void run() {
            byte[] fileContent = FileUtil.readFileContent(this.sourceFilePath);
            sendFileToReplica(this.client, fileContent, this.checksum, this.chunkOrder, this.numChunk, this.copyReplica, this.destPath);
        }
    }

    private class DeleteReplicaFolder implements Runnable {
        private ChatMessages.DeleteReplica replicaDelMessage;
        private ChannelHandlerContext ctx;

        public DeleteReplicaFolder(ChatMessages.DeleteReplica message, ChannelHandlerContext ctx) {
            this.replicaDelMessage = message;
            this.ctx = ctx;
        }

        @Override
        public void run() {
            StorageNodeUtil.deleteFolderInReplica(replicaDelMessage, storageDirectory);
        }
    }
}

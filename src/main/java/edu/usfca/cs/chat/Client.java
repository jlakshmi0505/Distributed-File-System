package edu.usfca.cs.chat;

import edu.usfca.cs.chat.util.ClientUtil;
import edu.usfca.cs.chat.util.ConnectionUtil;
import edu.usfca.cs.chat.util.Constants;
import edu.usfca.cs.chat.util.FileUtil;
import io.netty.channel.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;


@ChannelHandler.Sharable
public class Client
        extends SimpleChannelInboundHandler<ChatMessages.ChatMessagesWrapper> {


    private String username;
    private String hostname;
    private String tempDirectory = "";

    private static Channel controllerChannel;
    private ExecutorService sendFileReadMsgToSnExecutorService;

    private ExecutorService deleteChunksReqService;

    private AtomicInteger deleteChunksRespCount = new AtomicInteger(0);
    private AtomicInteger deleteChunksReqCount = new AtomicInteger(0);
    private AtomicBoolean isShutDown = new AtomicBoolean(false);

    private ConcurrentMap<String, boolean[]> chunkProcessedMap = new ConcurrentHashMap<>();
    private ConcurrentMap<String, List<Channel>> snChannels = new ConcurrentHashMap<>();
    private ConcurrentMap<String, List<Boolean>> storageNodesProcessedMap = new ConcurrentHashMap<>();

    private final static Logger log = Logger.getLogger("Client.class");

    public Client(String hostname, String username) {
        this.hostname = hostname;
        this.username = username;

        this.tempDirectory = FileUtil.getFileName("/tmp", FileUtil.randomString());
        FileUtil.makeDirectory(this.tempDirectory);
    }

    public static void main(String[] args)
            throws IOException {
        Client c = null;

        if (args.length >= 2) {
            c = new Client(args[0], args[1]);
        }

        if (c == null) {
            System.out.println("Usage: Client <hostname> <username>");
            System.exit(1);
        }

        c.controllerChannel = ConnectionUtil.connectController(c.hostname, c);
        ConnectionUtil.writeToChannel(c.controllerChannel, ClientUtil.sendRegistration(c.username));
        System.out.println(ClientUtil.clientUsageInfo());

        InputReader reader = new InputReader(c , c.username);
        Thread inputThread = new Thread(reader);
        inputThread.start();
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
                readFileFromStorageNode(fileMessage, ctx);
            }
            break;
            case STORAGENODEMETA: {
                ChatMessages.StorageNodeMeta metaInfo = msg.getStorageNodeMeta();
                ChatMessages.StorageNodeMeta.RequestCase requestCase = metaInfo.getRequestCase();
                switch (requestCase) {
                    case FILEREADREQUEST:
                        connectSNToReadFile(metaInfo.getFileReadRequest().getSourceFilePath(),
                                metaInfo.getFileReadRequest().getDestFilePath(),
                                metaInfo.getStorageNodesList());
                        break;
                    case FILEWRITEREQUEST:
                        if (metaInfo.getMsg().equals(Constants.DUPLICATE)) {
                            sendDeleteChunkReq(metaInfo);
                        } else
                            ClientUtil.breakFileToChunks(metaInfo, this);
                        break;
                }
            }
            break;
            case NOFILECONTENT: {
                updateStorageNodesProcessedMap(msg.getNoFileContent(), ctx);
            }
            break;
            case ERROR: {
                ChatMessages.Error errorMsg = msg.getError();
                if (errorMsg.getErrorType().equals(Constants.CHECK_SUM_ERROR)){
                    System.out.println(errorMsg.getErrorReason());
                    closeAllStorageNodeChannels(errorMsg.getDestFilePath());
                } else if (errorMsg.getErrorType().equals(Constants.WRITE)) {
                    ClientUtil.sendFileWriteRequest(errorMsg.getSourcePath(), errorMsg.getDestFilePath(),
                            controllerChannel);
                }else {
                    System.out.println(errorMsg.getErrorReason());
                }
            }
            break;
            case DELETECHUNKSRESP: {
                deleteChunksRespCount.incrementAndGet();
                if (isShutDown.get() && deleteChunksRespCount.get() == deleteChunksReqCount.get())
                    ClientUtil.sendFileWriteRequest(msg.getDeleteChunksResp().getSourcePath(),
                            msg.getDeleteChunksResp().getFilePath(), controllerChannel);
            }
            break;
            case ACTIVENODESINFO: {
                ChatMessages.ActiveNodesInfo activeNodesInfo = msg.getActiveNodesInfo();
                System.out.println(ClientUtil.printNodeInfo(activeNodesInfo));
            }
            break;
            case FILETREEINFO:{
                System.out.println(msg.getFileTreeInfo().getFileTree());
            }
            break;
        }
    }

    private void sendDeleteChunkReq(ChatMessages.StorageNodeMeta metaMsg) {
        int numStorageNodes = metaMsg.getStorageNodesCount();
        deleteChunksReqCount.getAndSet(numStorageNodes);
        deleteChunksRespCount.getAndSet(0);
        deleteChunksReqService = Executors.newFixedThreadPool(numStorageNodes);
        for (ChatMessages.StorageNodeMeta.StorageNode storageNode : metaMsg.getStorageNodesList()) {
            String hostName = storageNode.getUrl();
            int port = storageNode.getPort();
            Channel channel = ConnectionUtil.connectSN(hostName, port, this);
            if (channel != null) {
                SendDeleteChunksReqMsgToStorageNode getChunkOrder = new SendDeleteChunksReqMsgToStorageNode(metaMsg.getFileWriteRequest().getDestFilePath(),
                        metaMsg.getFileWriteRequest().getSourceFilePath(), channel);
                deleteChunksReqService.submit(getChunkOrder);

            }
        }
        isShutDown.getAndSet(true);
        deleteChunksReqService.shutdown();
    }

    private void updateStorageNodesProcessedMap(ChatMessages.NoFileContent noFileContent, ChannelHandlerContext ctx) {
        ExecutorService updateSnProcessedMapExecutorService = Executors.newCachedThreadPool();
        UpdateStorageNodesProcessedWithNoFile updateProcessedNodes = new UpdateStorageNodesProcessedWithNoFile(noFileContent, ctx);
        updateSnProcessedMapExecutorService.submit(updateProcessedNodes);
        updateSnProcessedMapExecutorService.shutdown();
    }

    private void readFileFromStorageNode(ChatMessages.FileContent fileMessage, ChannelHandlerContext ctx) {
        // thread
        ExecutorService fileContentFromSnExecutorService = Executors.newSingleThreadExecutor();
        RetriveFileFromStorageNode retrieveFile = new RetriveFileFromStorageNode(fileMessage, ctx);
        fileContentFromSnExecutorService.submit(retrieveFile);
        fileContentFromSnExecutorService.shutdown();
    }

    private void connectSNToReadFile(String sourceFilePath, String destFilePath,
                                     List<ChatMessages.StorageNodeMeta.StorageNode> storageNodeList) {
        int numStorageNodes = storageNodeList.size();
        String filePath = sourceFilePath;
//        try {
//            System.out.println("sleeping");
//            Thread.sleep(30000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        System.out.println("resuming");
        sendFileReadMsgToSnExecutorService = Executors.newFixedThreadPool(numStorageNodes);
        for (ChatMessages.StorageNodeMeta.StorageNode storageNode : storageNodeList) {
            String hostName = storageNode.getUrl();
            int port = storageNode.getPort();
            Channel snChannel = null;
            Channel channel = ConnectionUtil.connectSN(hostName, port, this);
            // channel is null if the connection to SN doesn't happen and in that case the client needs
            // to connect with the replica node to get the files
            if (channel == null) {
                log.log(Level.INFO, "Primary SN {0} connection failed. Connecting to Replica", hostName);
                List<ChatMessages.FileReplicaLocation.StorageNodeReplica> replicas = storageNode.getReplicas().getStorageNodeReplicaList();
                for (ChatMessages.FileReplicaLocation.StorageNodeReplica replica : replicas) {
                    channel = ConnectionUtil.connectSN(replica.getUrl(), replica.getPort(), this);
                    if (channel != null) {
                        log.log(Level.INFO, "Connected to Replica: {0}", replica.getUrl());
                        snChannel = channel;
                        filePath = FileUtil.getFileName(storageNode.getId(), sourceFilePath);
                        break;
                    }
                }
            } else {
                snChannel = channel;
                filePath = sourceFilePath;
            }
            if (snChannel != null) {
                SendReadFileMessageToStorageNode sendReadFileMsgToSN = new SendReadFileMessageToStorageNode(snChannel, filePath, destFilePath, numStorageNodes);
                sendFileReadMsgToSnExecutorService.submit(sendReadFileMsgToSN);
            }
        }
        sendFileReadMsgToSnExecutorService.shutdown();
    }

    synchronized private boolean mergeChunks(boolean[] chunks, String filePath) {
        if (FileUtil.andMap(chunks)) {
            Arrays.fill(chunks, false);
            // all chunks are ready
            String tempFilesRootDir = FileUtil.getFileName(tempDirectory, filePath);
            FileUtil.dirToFile(tempFilesRootDir, filePath, tempDirectory);
            return true;
        } else {
            return false;
        }
    }

    synchronized private boolean noFileContentInAllStorageNodes(List<Boolean> processedNodes, int numStorageNodes) {
        if (processedNodes.size() == numStorageNodes) {
            processedNodes.clear();
            return true;
        } else {
            return false;
        }
    }

   synchronized private void closeAllStorageNodeChannels(String filePath) {
        List<Channel> snChannelList = snChannels.get(filePath);
        // close all connection
        for (Channel snChannel : snChannelList) {
            ClientUtil.disconnectSN(snChannel);
        }
        snChannels.remove(filePath);
        storageNodesProcessedMap.remove(filePath);
        chunkProcessedMap.remove(filePath);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }

    private static class InputReader implements Runnable {
        private Client client;
        private String username;

        public InputReader(Client client, String username) {
            this.client = client;
            this.username = username;
        }

        public void run() {
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(System.in));

            while (true) {
                String line = "";
                try {
                    line = reader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                }
                if (line.startsWith("write")) {
                    //client.snChunkOrder.clear();
                    client.deleteChunksRespCount.getAndSet(0);
                    client.deleteChunksReqCount.getAndSet(0);
                    ClientUtil.sendFileReadRequest(controllerChannel ,line, Constants.WRITE);
                } else if (line.startsWith("read")) {
                    ClientUtil.sendFileReadRequest(controllerChannel ,line, Constants.READ);
                } else if (line.startsWith("info")) {
                    ClientUtil.sendDfsInfoRequest(controllerChannel, Constants.INFO);
                } else if (line.startsWith("ls")){
                    ClientUtil.sendDfsInfoRequest(controllerChannel, Constants.LS);
                }
            }
        }
    }

    private class SendReadFileMessageToStorageNode implements Runnable {
        private String sourceFile;
        private String destFile;
        private Channel snChannel;
        private int numStorageNodes;

        SendReadFileMessageToStorageNode(Channel snChannel, String sourceFile, String destFile, int numStorageNodes) {
            this.sourceFile = sourceFile;
            this.destFile = destFile;
            this.snChannel = snChannel;
            this.numStorageNodes = numStorageNodes;
        }

        @Override
        public void run() {
            if (!snChannels.containsKey(destFile)) {
                List<Channel> snList = new ArrayList<>();
                snChannels.put(destFile, snList);
            }
            snChannels.get(destFile).add(snChannel);
            ClientUtil.sendReadFileMessageToStorageNode(snChannel, sourceFile, destFile, numStorageNodes);
        }
    }

    private class SendDeleteChunksReqMsgToStorageNode implements Runnable {
        private String filePath;
        private String sourceFile;
        Channel snChannel;

        SendDeleteChunksReqMsgToStorageNode(String filePath, String sourceFile, Channel snChannel) {
            this.filePath = filePath;
            this.sourceFile = sourceFile;
            this.snChannel = snChannel;
        }

        @Override
        public void run() {
            ClientUtil.sendDeleteChunksReqMsgToSN(snChannel, filePath, sourceFile);
        }
    }

    private class RetriveFileFromStorageNode implements Runnable {
        private ChatMessages.FileContent fileMessage;
        private ChannelHandlerContext snChannelHandler;

        RetriveFileFromStorageNode(ChatMessages.FileContent fileContent, ChannelHandlerContext snChannelHandler) {
            this.fileMessage = fileContent;
            this.snChannelHandler = snChannelHandler;
        }

        @Override
        public void run() {
            String filePath = fileMessage.getFilePath();

            int numChunks = fileMessage.getNumChunks();
            int chunkOrder = fileMessage.getChunkOrder();
            if (!chunkProcessedMap.containsKey(filePath)) {
                boolean[] chunks = new boolean[numChunks];
                chunkProcessedMap.put(filePath, chunks);
            }
            boolean[] chunks = chunkProcessedMap.get(filePath);
            // write the file chunk as a temp file in the tempdirectory
            byte[] fileContent = fileMessage.getFileContent().toByteArray();
            String formattedChunkOrder = String.format("%04d", chunkOrder);
            String tempFilePath = FileUtil.getFileName(tempDirectory, filePath, formattedChunkOrder);
            FileUtil.writeFileContent(tempFilePath, fileContent);
            chunks[chunkOrder] = true;
            if (mergeChunks(chunks, filePath)) {
                closeAllStorageNodeChannels(filePath);
                System.out.println("Content retrieved");
            }
        }
    }

    private class UpdateStorageNodesProcessedWithNoFile implements Runnable {
        private ChatMessages.NoFileContent fileMessage;
        private ChannelHandlerContext snChannelHandler;

        UpdateStorageNodesProcessedWithNoFile(ChatMessages.NoFileContent fileContent, ChannelHandlerContext snChannelHandler) {
            this.fileMessage = fileContent;
            this.snChannelHandler = snChannelHandler;
        }

        @Override
        public void run() {
            log.log(Level.INFO, "File doesn't exist in SN {0}",
                    new Object[]{snChannelHandler.channel().localAddress()});
            String filePath = fileMessage.getDestPath();

            if (!storageNodesProcessedMap.containsKey(filePath)) {
                List<Boolean> storageNodes = new ArrayList<>();
                storageNodesProcessedMap.put(filePath, storageNodes);
            }
            storageNodesProcessedMap.get(filePath).add(true);
            int numStorageNodes = fileMessage.getTotalStorageNodes();
            // check if all the storage nodes return noFileContent message
            if (noFileContentInAllStorageNodes(storageNodesProcessedMap.get(filePath), numStorageNodes)) {
                log.log(Level.INFO, "All the SNs contacted didn't contain the file");
                closeAllStorageNodeChannels(filePath);
                System.out.println("File doesn't exist");
            }
        }
    }
}


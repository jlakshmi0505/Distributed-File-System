package edu.usfca.cs.chat;

import com.google.protobuf.ByteString;
import edu.usfca.cs.chat.net.ServerMessageRouter;
import edu.usfca.cs.chat.util.*;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.struct.FileStat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;


@ChannelHandler.Sharable
public class Controller
        extends SimpleChannelInboundHandler<ChatMessages.ChatMessagesWrapper> {

    private static final Logger logger = Logger.getLogger("DFS");

    private ExecutorService esReadDirReq;
    private ExecutorService esGetAttrReq;
    private ExecutorService esOpenReq;
    private ExecutorService esReadReq;

    private Tree fileSystemRoot;

    ServerMessageRouter messageRouter;
    private String rootDir = ".";

    Map<ChannelHandlerContext, ChatMessages.Heartbeat> storageNodeToHeartbeat;
    Map<ChannelHandlerContext, Integer> storageNodeToPort;
    Map<ChannelHandlerContext, BloomFilter> storageNodeToBloomFilter;
    ConcurrentHashMap<ChannelHandlerContext, HashSet<ChannelHandlerContext>> storageNodeToReplicas;
    ConcurrentHashMap<ChannelHandlerContext, Set<ChannelHandlerContext>> replicaToPrimaryNodes;
    Map<ChannelHandlerContext, String> storageNodeToId;
    HashMap<String, ChannelHandlerContext> clientToCtx;
    HashMap<ChannelHandlerContext, String> ctxToClient;
    ConcurrentHashMap<ChannelHandlerContext, ChannelHandlerContext> newlyAddedReplica;
    private static final int NUMBER_OF_REPLICAS = 2;
    private static ConcurrentHashMap<ChannelHandlerContext, Long> heartbeatLastTimeStamp;
    private static boolean isReplicaCreated = false;
    private static ScheduledExecutorService checkStorageNodeHeartbeat;

    public Controller() {
        clientToCtx = new HashMap<>();
        ctxToClient = new HashMap<>();
        storageNodeToHeartbeat = new HashMap<>();
        heartbeatLastTimeStamp = new ConcurrentHashMap<>();
        storageNodeToBloomFilter = new HashMap<>();
        storageNodeToReplicas = new ConcurrentHashMap<>();
        replicaToPrimaryNodes = new ConcurrentHashMap<>();
        storageNodeToPort = new ConcurrentHashMap<>();
        storageNodeToId = new HashMap<>();
        newlyAddedReplica = new ConcurrentHashMap<>();
        fileSystemRoot = new Tree("/");
    }

    public void start(int port, String rootDir) throws IOException {
        this.rootDir = rootDir;
        messageRouter = new ServerMessageRouter(this);
        messageRouter.listen(port);
        System.out.println("Listening on port " + port + "...");
        System.out.println("Serving files from " + this.rootDir);
    }

    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            System.out.println("Usage: Controller <root_dir>");
            System.exit(1);
        }

        Controller c = new Controller();
        c.start(ConfigUtil.controllerPort, args[0]);

        //check storage node failure every 1 min
        Runnable runnable = () -> c.checkStorageNodeFailure();
        checkStorageNodeHeartbeat = Executors.newSingleThreadScheduledExecutor();
        checkStorageNodeHeartbeat.scheduleAtFixedRate(runnable, 1, 1, TimeUnit.MINUTES);
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
        if (storageNodeToPort.containsKey(ctx)) {
            logger.log(Level.INFO, "Node failed : {0}", addr.getHostName() + " : " + storageNodeToPort.get(ctx));
            storageNodeToPort.remove(ctx);
            storageNodeToBloomFilter.remove(ctx);
            if (storageNodeToHeartbeat.containsKey(ctx)) {
                storageNodeToHeartbeat.remove(ctx);
            }
            if (!isReplicaCreated) {
                if (storageNodeToReplicas.containsKey(ctx)) {
                    storageNodeToReplicas.remove(ctx);
                }
                if (replicaToPrimaryNodes.containsKey(ctx)) {
                    replicaToPrimaryNodes.remove(ctx);
                }
                if (storageNodeToId.containsKey(ctx)) {
                    storageNodeToId.remove(ctx);
                }
            }

        }
        String username = ctxToClient.get(ctx);
        clientToCtx.remove(username);
        ctxToClient.remove(ctx);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
            throws Exception {
        /* Writable status of the channel changed */
    }


    private void readRegistrationMsg(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) {
        String username = msg.getRegistration().getUsername();
        if (!ControllerUtil.isDuplicateUsername(username, ctx, clientToCtx)) {
            clientToCtx.put(username, ctx);
            ctxToClient.put(ctx, username);
        } else {
            ControllerUtil.sendChatMessage(username, "Registration Failed", ctx.channel());
            ctx.close();
        }
    }

    private void readSNRegistrationMsg(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) {
        storageNodeToPort.put(ctx, msg.getStorageNodeRegistration().getPort());
        BloomFilter bloomFilter = new BloomFilter(10000, 10);
        storageNodeToBloomFilter.put(ctx, bloomFilter);
        storageNodeToId.put(ctx, msg.getStorageNodeRegistration().getId());
        addReplica(ctx);
    }

    private void addReplica(ChannelHandlerContext ctx) {
        int no_of_storage_nodes = storageNodeToPort.size();
        storageNodeToReplicas.put(ctx, new HashSet<>());
        //Not adding replicas if storage node count is less than 4
        if (no_of_storage_nodes < 4) {
            return;
            //if storage node count is equal to 4 we wil add replica for all the previous 3 nodes and the newly added node
        } else if (no_of_storage_nodes == 4) {
            for (ChannelHandlerContext c : storageNodeToReplicas.keySet()) {
                createReplica(c);
                ControllerUtil.sendReplicaNodeInfoMsgToSN(c, storageNodeToReplicas, storageNodeToPort);
            }
            // if storage node count is more than 4 then just add replica for newly added node
        } else {
            createReplica(ctx);
            ControllerUtil.sendReplicaNodeInfoMsgToSN(ctx, storageNodeToReplicas, storageNodeToPort);
        }
    }

    private void createReplica(ChannelHandlerContext ctx) {
        isReplicaCreated = true;
        Random rand = new Random();
        Set<ChannelHandlerContext> givenList = new HashSet<>(storageNodeToPort.keySet());
        Set<ChannelHandlerContext> listOfReplica = new HashSet<>();
        givenList.remove(ctx);
        int numberOfReplicas = NUMBER_OF_REPLICAS;
        //if one replica is already there
        if (storageNodeToReplicas.get(ctx).size() == 1) {
            listOfReplica = (Set<ChannelHandlerContext>) storageNodeToReplicas.get(ctx);
            numberOfReplicas -= 1;
            givenList.removeAll(listOfReplica);
        }
        if (givenList.size() > 0) {
            List<ChannelHandlerContext> stringsList = new ArrayList<>(givenList);
            for (int i = 0; i < numberOfReplicas; i++) {
                int randomIndex = rand.nextInt(stringsList.size());
                ChannelHandlerContext randomElement = stringsList.get(randomIndex);
                listOfReplica.add(randomElement);
                newlyAddedReplica.put(ctx, randomElement);
                updateReplicaMap(ctx, randomElement);
                stringsList.remove(randomElement);
            }
        }
        storageNodeToReplicas.get(ctx).addAll(listOfReplica);
    }

    private void updateReplicaMap(ChannelHandlerContext ctx, ChannelHandlerContext randomElement) {
        if (replicaToPrimaryNodes.containsKey(randomElement)) {
            Set<ChannelHandlerContext> listOfPrimaryNodes = replicaToPrimaryNodes.get(randomElement);
            listOfPrimaryNodes.add(ctx);
        } else {
            Set<ChannelHandlerContext> listOfPrimaryNodes = new HashSet<>();
            listOfPrimaryNodes.add(ctx);
            replicaToPrimaryNodes.put(randomElement, listOfPrimaryNodes);
        }
    }

    private void readHeartbeat(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) {
        double freeSpace = msg.getHeartbeat().getFreeSpaceAvailable();
        System.out.printf("Free space on disk: %s mb%n", freeSpace);
        System.out.println("Requests processed count: " + msg.getHeartbeat().getRequestsProcessedCount());
        storageNodeToHeartbeat.put(ctx, msg.getHeartbeat());
        updateStorageNodeLastHeartbeatTime(ctx);
    }

    private void updateStorageNodeLastHeartbeatTime(ChannelHandlerContext ctx) {
        if (heartbeatLastTimeStamp.containsKey(ctx)) {
            long s = heartbeatLastTimeStamp.get(ctx);
            heartbeatLastTimeStamp.put(ctx, System.currentTimeMillis() / 1000);
        } else {
            heartbeatLastTimeStamp.put(ctx, (long) 0);
        }
    }

    private void updateStorageNodeBloomFilter(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) {
        String filePath = msg.getFileWriteAck().getFilePath();
        System.out.println(filePath);
        storageNodeToBloomFilter.get(ctx).put(filePath.getBytes());
        fileSystemRoot.addFilePath(filePath);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) {
        //System.out.println("msg = " + msg.getMsgCase());
        /* Hmm... */
        ChatMessages.ChatMessagesWrapper.MsgCase msgCase = msg.getMsgCase();
        switch (msgCase) {
            case REGISTRATION:
                readRegistrationMsg(ctx, msg);
                break;
            case STORAGENODEREGISTRATION:
                readSNRegistrationMsg(ctx, msg);
                break;
            case FILEWRITEREQUEST:
                ControllerUtil.sendStorageNodesMetaMsgToWrite(msg, ctx.channel(),
                        storageNodeToHeartbeat, storageNodeToId,
                        storageNodeToReplicas, storageNodeToPort);
                break;
            case FILEWRITEACK:
                updateStorageNodeBloomFilter(ctx, msg);
                break;
            case HEARTBEAT:
                readHeartbeat(ctx, msg);
                break;
            case FILEREADREQUEST:
                ControllerUtil.sendStorageNodesMetaMsgToRead(ctx.channel(), msg,
                        storageNodeToHeartbeat, storageNodeToId,
                        storageNodeToReplicas, storageNodeToPort,
                        storageNodeToBloomFilter);
                break;
            case REQUESTINFO:
                if (msg.getRequestInfo().getInfoType().equals(Constants.INFO)) {
                    ControllerUtil.sendActiveNodesInfo(ctx.channel(), storageNodeToHeartbeat, storageNodeToId);
                }else{
                    ControllerUtil.sendFileSystemTree(ctx.channel(), fileSystemRoot.toString());
                }
                break;
            /** Handle posix client requests in a new thread */
            case READDIR_REQ:
                esReadDirReq = Executors.newSingleThreadExecutor();
                handlePosixClientRequest(esReadDirReq, ctx, msg);
                break;
            case GETATTR_REQ:
                esGetAttrReq = Executors.newSingleThreadExecutor();
                handlePosixClientRequest(esGetAttrReq, ctx, msg);
                break;
            case OPEN_REQ:
                esOpenReq = Executors.newSingleThreadExecutor();
                handlePosixClientRequest(esOpenReq, ctx, msg);
                break;
            case READ_REQ:
                esReadReq = Executors.newSingleThreadExecutor();
                handlePosixClientRequest(esReadReq, ctx, msg);
                break;
        }
    }

    private void handlePosixClientRequest(ExecutorService executorServiceFS, ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) {
        PosixClientRequest posixClientRequest = new PosixClientRequest(ctx, msg);
        executorServiceFS.submit(posixClientRequest);
        executorServiceFS.shutdown();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }

    private class PosixClientRequest implements Runnable {

        private ChatMessages.ChatMessagesWrapper msg;
        private ChannelHandlerContext ctx;

        PosixClientRequest(ChannelHandlerContext ctx, ChatMessages.ChatMessagesWrapper msg) {
            this.ctx = ctx;
            this.msg = msg;
        }

        private void sendReadDirResp(ChannelHandlerContext ctx, ChatMessages.ReaddirRequest readDir) {
            File reqPath = new File(rootDir + readDir.getPath());

            logger.log(Level.INFO, "readdir: {0}", reqPath);

            ChatMessages.ReaddirResponse.Builder respBuilder
                    = ChatMessages.ReaddirResponse.newBuilder();
            respBuilder.setStatus(0);

            try {
                Files.newDirectoryStream(reqPath.toPath())
                        .forEach(path -> respBuilder.addContents(
                                path.getFileName().toString()));
            } catch (IOException e) {
                logger.log(Level.WARNING, "Error reading directory", e);
                respBuilder.setStatus(-ErrorCodes.ENOENT());
            }

            ChatMessages.ReaddirResponse resp = respBuilder.build();
            ChatMessages.ChatMessagesWrapper wrapper
                    = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setReaddirResp(resp)
                    .build();

            ctx.writeAndFlush(wrapper);

            logger.log(Level.INFO, "readdir status: {0}", resp.getStatus());

        }

        private void sendGetAttrReq(ChannelHandlerContext ctx, ChatMessages.GetattrRequest getattr) {
            File reqPath = new File(rootDir + getattr.getPath());

            logger.log(Level.INFO, "getattr: {0}", reqPath);

            ChatMessages.GetattrResponse.Builder respBuilder
                    = ChatMessages.GetattrResponse.newBuilder();
            respBuilder.setStatus(0);

            Set<PosixFilePermission> permissions = null;
            try {
                permissions = Files.getPosixFilePermissions(
                        reqPath.toPath(), LinkOption.NOFOLLOW_LINKS);
            } catch (IOException e) {
                logger.log(Level.WARNING, "Error reading file attributes", e);
                respBuilder.setStatus(-ErrorCodes.ENOENT());
            }

            int mode = 0;
            if (permissions != null) {
                for (PosixFilePermission perm : PosixFilePermission.values()) {
                    mode = mode << 1;
                    mode += permissions.contains(perm) ? 1 : 0;
                }
            }

            if (reqPath.isDirectory()) {
                mode = mode | FileStat.S_IFDIR;
            } else {
                mode = mode | FileStat.S_IFREG;
            }

            respBuilder.setSize(reqPath.length());
            respBuilder.setMode(mode);

            ChatMessages.GetattrResponse resp = respBuilder.build();

            ChatMessages.ChatMessagesWrapper wrapper
                    = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setGetattrResp(resp)
                    .build();

            ctx.writeAndFlush(wrapper);
            logger.log(Level.INFO, "getattr status: {0}", resp.getStatus());
        }

        private void sendOpenReq(ChannelHandlerContext ctx, ChatMessages.OpenRequest open) {
            File reqPath = new File(rootDir + open.getPath());

            logger.log(Level.INFO, "open: {0}", reqPath);

            ChatMessages.OpenResponse.Builder respBuilder
                    = ChatMessages.OpenResponse.newBuilder();
            respBuilder.setStatus(0);

            if (Files.isRegularFile(reqPath.toPath(), LinkOption.NOFOLLOW_LINKS) == false) {
                respBuilder.setStatus(-ErrorCodes.ENOENT());
            }

            ChatMessages.OpenResponse resp = respBuilder.build();

            ChatMessages.ChatMessagesWrapper wrapper
                    = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setOpenResp(resp)
                    .build();

            ctx.writeAndFlush(wrapper);
            logger.log(Level.INFO, "open status: {0}", resp.getStatus());
        }

        private void sendReadReq(ChannelHandlerContext ctx, ChatMessages.ReadRequest read) {
            File reqPath = new File(rootDir + read.getPath());

            logger.log(Level.INFO, "read: {0}", reqPath);

            ChatMessages.ReadResponse.Builder respBuilder
                    = ChatMessages.ReadResponse.newBuilder();

            try (FileInputStream fin = new FileInputStream(reqPath)) {
                fin.skip(read.getOffset());
                byte[] contents = new byte[(int) read.getSize()];
                int readSize = fin.read(contents);
                respBuilder.setContents(ByteString.copyFrom(contents));
                respBuilder.setStatus(readSize);
            } catch (IOException e) {
                logger.log(Level.WARNING, "Error reading file", e);
                respBuilder.setStatus(0);
            }

            ChatMessages.ReadResponse resp = respBuilder.build();

            ChatMessages.ChatMessagesWrapper wrapper
                    = ChatMessages.ChatMessagesWrapper.newBuilder()
                    .setReadResp(resp)
                    .build();

            ctx.writeAndFlush(wrapper);
            logger.log(Level.INFO, "read status: {0}", resp.getStatus());
        }

        @Override
        public void run() {
            ChatMessages.ChatMessagesWrapper.MsgCase msgCase = msg.getMsgCase();
            switch (msgCase) {
                case READDIR_REQ:
                    sendReadDirResp(ctx, msg.getReaddirReq());
                    break;
                case GETATTR_REQ:
                    sendGetAttrReq(ctx, msg.getGetattrReq());
                    break;
                case OPEN_REQ:
                    sendOpenReq(ctx, msg.getOpenReq());
                    break;
                case READ_REQ:
                    sendReadReq(ctx, msg.getReadReq());
                    break;
            }
        }
    }

    private void checkStorageNodeFailure() {
        logger.log(Level.INFO, "Checking is there any node failure happened in last 1 min ?  ");
        for (ChannelHandlerContext ctx : heartbeatLastTimeStamp.keySet()) {
            long s = heartbeatLastTimeStamp.get(ctx);
            long diff = ((System.currentTimeMillis() / 1000) - s);
            if (diff >= 60 && (!storageNodeToPort.containsKey(ctx)) && isReplicaCreated) {
                logger.log(Level.INFO, " Oh no !! Node failed !! :( ");
                heartbeatLastTimeStamp.remove(ctx);
                ExecutorService nodeFailureExecutors = Executors.newCachedThreadPool();
                Future future = nodeFailureExecutors.submit(new HandleNodeFailure(ctx));
                logger.log(Level.INFO, "Started handling replica maintenance !! ");
                while (!future.isDone()) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                nodeFailureExecutors.shutdown();
            } else {
                logger.log(Level.INFO, "Node is up and running :)");
            }
        }
    }

    private void handleNodeFailure(ChannelHandlerContext ctx) {
        if (replicaToPrimaryNodes.containsKey(ctx)) {
            ExecutorService secondaryNodeFailureExecutors = Executors.newCachedThreadPool();
            Future future = secondaryNodeFailureExecutors.submit(new HandleSecondaryNodeFailure(ctx));
            while (!future.isDone()) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            secondaryNodeFailureExecutors.shutdown();
        }
        if (storageNodeToReplicas.containsKey(ctx)) {
            ExecutorService primaryNodeFailureExecutors = Executors.newCachedThreadPool();
            Future future = primaryNodeFailureExecutors.submit(new HandlePrimaryNodeFailure(ctx));
            while (!future.isDone()) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            primaryNodeFailureExecutors.shutdown();
        }
    }

    private void handleSecondaryNodeFailure(ChannelHandlerContext ctx) {
        if (replicaToPrimaryNodes.containsKey(ctx)) {
            ExecutorService copyFilesExecutors = Executors.newCachedThreadPool();
            Set<ChannelHandlerContext> setOfPrimaryNodes = replicaToPrimaryNodes.get(ctx);
            if (setOfPrimaryNodes.size() > 0) {
                logger.log(Level.INFO, "Started handling replica maintenance for primary nodes of failed node!! ");
                ExecutorService replicaUpdateExecutors = Executors.newFixedThreadPool(setOfPrimaryNodes.size());
                for (ChannelHandlerContext primaryCtx : setOfPrimaryNodes) {
                    if (storageNodeToPort.containsKey(primaryCtx)) {
                        if (storageNodeToReplicas.containsKey(primaryCtx)) {
                            Set<ChannelHandlerContext> setOfReplicas = storageNodeToReplicas.get(primaryCtx);
                            setOfReplicas.remove(ctx);
                        }
                        InetSocketAddress addr = (InetSocketAddress) primaryCtx.channel().remoteAddress();
                        logger.log(Level.INFO, "Creating new replica for primary node {0} of failed node ", addr.getHostName() + " : " + storageNodeToPort.get(primaryCtx));
                        createReplica(primaryCtx);
                        replicaUpdateExecutors.submit(new SendReplicaUpdatedInfo(primaryCtx));
                    }
                }
                replicaUpdateExecutors.shutdown();
                logger.log(Level.INFO, "Copying files to newly created replica ");
                Future future = copyFilesExecutors.submit((new CopyFilesSecondaryReplica(ctx, setOfPrimaryNodes)));
                while (!future.isDone()) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                replicaToPrimaryNodes.remove(ctx);
                copyFilesExecutors.shutdown();
            }
        }
    }

    private void copyFilesInCaseOfPrimaryNodeFailure(ChannelHandlerContext failedNodeCtx, ChannelHandlerContext replicaOfFailedPrimary,
                                                     ChannelHandlerContext newPrimaryNode, String parentId) {
        if (newPrimaryNode != null && replicaOfFailedPrimary != null) {
            if (storageNodeToPort != null && storageNodeToPort.containsKey(newPrimaryNode)) {
                int port = storageNodeToPort.get(newPrimaryNode);
                InetSocketAddress addr = (InetSocketAddress) newPrimaryNode.channel().remoteAddress();
                logger.log(Level.INFO, "Copying files to newly created Primary node {0} ", addr.getHostName() + " : " + port);
                ChatMessages.CopyReplica copyReplica
                        = ChatMessages.CopyReplica.newBuilder()
                        .setHostName(addr.getHostName())
                        .setPort(port)
                        .setParentId(parentId)
                        .setFileType(Constants.PRIMARY)
                        .build();
                ChatMessages.ChatMessagesWrapper msgWrapper =
                        ChatMessages.ChatMessagesWrapper.newBuilder()
                                .setCopyReplica(copyReplica)
                                .build();

                if (replicaOfFailedPrimary.channel().isActive()) {
                    ChannelFuture write = replicaOfFailedPrimary.channel().writeAndFlush(msgWrapper);
                    write.syncUninterruptibly();
                }
            }
        }
    }

    private void copyFilesFromFailedSecondaryReplica(ChannelHandlerContext failedNodeCtx, Set<ChannelHandlerContext> setOfPrimaryNodes) {
        ChannelHandlerContext alreadyExistedReplica = null;
        for (ChannelHandlerContext primaryCtx : setOfPrimaryNodes) {
            ChannelHandlerContext newReplica = newlyAddedReplica.get(primaryCtx);
            Set<ChannelHandlerContext> currentReplicaMap = (Set<ChannelHandlerContext>) storageNodeToReplicas.get(primaryCtx);
            for (ChannelHandlerContext ct : currentReplicaMap) {
                if (!(ct.equals(newReplica))) {
                    alreadyExistedReplica = ct;
                }
            }
            if (newReplica != null && alreadyExistedReplica != null) {
                InetSocketAddress addr = null;
                if (storageNodeToPort != null) {
                    int port = storageNodeToPort.get(newReplica);
                    addr = (InetSocketAddress) newReplica.channel().remoteAddress();
                    ChatMessages.CopyReplica copyReplica
                            = ChatMessages.CopyReplica.newBuilder()
                            .setHostName(addr.getHostName())
                            .setPort(port)
                            .setParentId(storageNodeToId.get(primaryCtx))
                            .setFileType(Constants.SECONDARY)
                            .build();
                    ChatMessages.ChatMessagesWrapper msgWrapper =
                            ChatMessages.ChatMessagesWrapper.newBuilder()
                                    .setCopyReplica(copyReplica)
                                    .build();

                    if (alreadyExistedReplica.channel().isActive()) {
                        ChannelFuture write = alreadyExistedReplica.channel().writeAndFlush(msgWrapper);
                        write.syncUninterruptibly();
                    }
                }
            }
        }
    }

    private void handlePrimaryNodeFailure(ChannelHandlerContext ctx) throws ExecutionException, InterruptedException {
        ChannelHandlerContext newPrimaryNode = null;
        ChannelHandlerContext replicaOfFailedPrimary = null;
        Set<ChannelHandlerContext> setOfReplicaNodes = new HashSet<>();
        ExecutorService copyPrimaryFilesExecutors = Executors.newCachedThreadPool();
        if (storageNodeToReplicas.containsKey(ctx)) {
            setOfReplicaNodes = storageNodeToReplicas.get(ctx);
            if (setOfReplicaNodes.size() > 0) {
                newPrimaryNode = setOfReplicaNodes.iterator().next();
            }
            for (ChannelHandlerContext secondaryCtx : setOfReplicaNodes) {
                if (replicaToPrimaryNodes.containsKey(secondaryCtx) && storageNodeToPort.containsKey(secondaryCtx)) {
                    Set<ChannelHandlerContext> setOfPrimaryNodes = replicaToPrimaryNodes.get(secondaryCtx);
                    setOfPrimaryNodes.remove(ctx);
                }
            }
            storageNodeToReplicas.remove(ctx);
        }
        String parent_id = storageNodeToId.get(ctx);
        storageNodeToId.remove(parent_id);
        setOfReplicaNodes.remove(newPrimaryNode);
        if (setOfReplicaNodes.size() > 0) {
            replicaOfFailedPrimary = setOfReplicaNodes.iterator().next();
        }
        //copy files from failed primary replica to new replica
        Future future = copyPrimaryFilesExecutors.submit((new CopyFilesForPrimary(ctx, replicaOfFailedPrimary, newPrimaryNode, parent_id)));

        while (!future.isDone()) {
            Thread.sleep(200);
        }
        copyPrimaryFilesExecutors.shutdown();
        //deleteFailedParentFolder(replicaOfFailedPrimary,parent_id);
        //deleteFailedParentFolder(newPrimaryNode,parent_id);

    }

    private class CopyFilesForPrimary implements Runnable {
        private ChannelHandlerContext failedNodeCtx;
        private ChannelHandlerContext replicaOfFailedPrimary;
        private ChannelHandlerContext newPrimaryNode;
        private String parentId;

        public CopyFilesForPrimary(ChannelHandlerContext failedNodeCtx, ChannelHandlerContext replicaOfFailedPrimary, ChannelHandlerContext newPrimaryNode, String parentId) {
            this.failedNodeCtx = failedNodeCtx;
            this.replicaOfFailedPrimary = replicaOfFailedPrimary;
            this.newPrimaryNode = newPrimaryNode;
            this.parentId = parentId;
        }

        @Override
        public void run() {
            copyFilesInCaseOfPrimaryNodeFailure(failedNodeCtx, replicaOfFailedPrimary, newPrimaryNode, parentId);
        }
    }


    private void deleteFailedParentFolder(ChannelHandlerContext c, String parentId) {
        if (storageNodeToPort != null) {
            ChatMessages.DeleteReplica delReplica
                    = ChatMessages.DeleteReplica.newBuilder()
                    .setParentId(parentId)
                    .setFileType(Constants.SECONDARY)
                    .build();
            ChatMessages.ChatMessagesWrapper msgWrapper =
                    ChatMessages.ChatMessagesWrapper.newBuilder()
                            .setDeleteReplica(delReplica)
                            .build();

            if (c.channel().isActive()) {
                ChannelFuture write = c.channel().writeAndFlush(msgWrapper);
                write.syncUninterruptibly();
            }
        }
    }

    private class HandleSecondaryNodeFailure implements Runnable {
        private ChannelHandlerContext context;

        public HandleSecondaryNodeFailure(ChannelHandlerContext context) {
            this.context = context;
        }

        @Override
        public void run() {
            handleSecondaryNodeFailure(context);

        }
    }

    private class HandlePrimaryNodeFailure implements Runnable {
        private ChannelHandlerContext context;

        public HandlePrimaryNodeFailure(ChannelHandlerContext context) {
            this.context = context;
        }

        @Override
        public void run() {
            try {
                handlePrimaryNodeFailure(context);
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private class CopyFilesSecondaryReplica implements Runnable {
        private ChannelHandlerContext failedNodeCtx;
        private Set<ChannelHandlerContext> setOfPrimaryNodes;

        public CopyFilesSecondaryReplica(ChannelHandlerContext failedNodeCtx, Set<ChannelHandlerContext> setOfPrimaryNodes) {
            this.failedNodeCtx = failedNodeCtx;
            this.setOfPrimaryNodes = setOfPrimaryNodes;
        }

        @Override
        public void run() {
            copyFilesFromFailedSecondaryReplica(failedNodeCtx, setOfPrimaryNodes);
        }
    }

    private class HandleNodeFailure implements Runnable {
        private ChannelHandlerContext context;

        public HandleNodeFailure(ChannelHandlerContext context) {
            this.context = context;
        }

        @Override
        public void run() {
            handleNodeFailure(context);

        }
    }

    private class SendReplicaUpdatedInfo implements Runnable {
        private ChannelHandlerContext context;

        public SendReplicaUpdatedInfo(ChannelHandlerContext context) {
            this.context = context;
        }

        @Override
        public void run() {
            ControllerUtil.sendReplicaNodeInfoMsgToSN(context, storageNodeToReplicas, storageNodeToPort);
        }
    }
}

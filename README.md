# Distributed-File-System
Built a distributed file system (DFS) based on the technologiesfrom Amazon, Google, and others. DFS will support multiple storage nodes responsible for managing data.

## Key features include:

### POSIX Compatibility: unlike many other DFS, ours will be POSIX-compatible, meaning that the file system can be mounted like any other disk on the host operating system.
### Probabilistic Routing: to enable lookups without requiring excessive RAM, client requests will be routed probabilistically to relevant storage nodes via bloom filters.
### Parallel retrievals: large files will be split into multiple chunks. Client applications retrieve these chunks in parallel using threads.
### Interoperability: the DFS will use Google Protocol Buffers to serialize messages. Do not use Java serialization. This allows other applications to easily implement your wire format.
### Asynchronous Scalability: we will use non-blocking I/O to ensure your DFS can scale to handle hundreds of active client connections concurrently.
### Fault tolerance: your system must be able to detect and withstand two concurrent storage node failures and continue operating normally. It will also be able to recover corrupted files.

## Components:

Controller
Storage Node
Client



## Controller

The Controller is responsible for managing resources in the system, somewhat like an HDFS NameNode. When a new storage node joins your DFS, the first thing it does is contact the Controller. At a minimum, the Controller contains the following data structures:

A list of active storage nodes
The file system tree, describing the directories in your file system but NOT the files
A routing table for each directory in the file system tree with one or more bloom filters for probabilistic file lookups
Since this is probabilistic, the Controller will not know exactly where files are stored, but it will be able to route requests to their correct destination with a low probability of false positives.
When clients wish to store a new file, they will send a storage request to the controller, and it will reply with a list of destination storage nodes (plus replica locations) to send the chunks to. The Controller itself should never see any of the actual files, only their metadata.

To maintain the per-directory routing table, you will implement a bloom filter of file names stored there, one per storage node. When the controller receives a retrieval request from a client, it will query the bloom filter associated with the directory in question and return a list of matching nodes (due to the nature of bloom filters, this may include false positives).

The Controller is also responsible for detecting storage node failures and ensuring the system replication level is maintained. In your DFS, every chunk will be replicated twice for a total of 3 duplicate chunks. This means if a system goes down, you can re-route retrievals to a backup copy. You’ll also maintain the replication level by creating more copies in the event of a failure. You will need to design an algorithm for determining replica placement.

## Storage Node
Storage nodes are responsible for storing and retrieving file chunks. When a chunk is stored, it will be checksummed so on-disk corruption can be detected. When a corrupted file is retrieved, it should be repaired by requesting a replica before fulfilling the client request. Metadata, such as checksums, should be stored alongside the files on disk.

The storage nodes will send a heartbeat to the controller periodically to let it know that they are still alive. Every 5 seconds is a good interval for sending these. The heartbeat contains the free space available at the node and the total number of requests processed (storage, retrievals, etc.).

On startup: provide a storage directory path and the hostname/IP of the controller. Any old files present in the storage directory should be removed.

## Basic Client
You will build a basic client that allows storage and retrievals. Its functions include:

Breaking files into chunks, asking the controller where to store them, and then sending them to the appropriate storage node(s).
Note: Once the first chunk has been transferred to its destination storage node, that node will pass replicas along in a pipeline fashion. The client should not send each chunk 3 times.
If a file already exists, replace it with the new file. If the new file is smaller than the old, you are not required to remove old chunks (but file retrieval should provide the correct data).
Retrieving files in parallel. Each chunk in the file being retrieved will be requested and transferred on a separate thread. Once the chunks are retrieved, the file is reconstructed on the client machine.
The client will also be able to print out a list of active nodes (retrieved from the controller) and the total disk space available in the cluster (in GB), and number of requests handled by each node.

NOTE: Your client must either accept command line arguments or provide its own text-based command entry interface. Recompiling your client to execute different actions is not allowed and will incur a 5 point deduction.

POSIX Client
This client implements a POSIX-compatible file system via FUSE. It receives file system instructions from the FUSE library and translates them to messages your DFS can understand. This will allow you to mount your DFS just as you would a flash drive, hard disk drive, etc.


## Controller

 File system tree
Directory routing table (Bloom Filter implementation and lookup functionality)
Node failure detection
Coordinating replica maintenance
Storage node implementation:

Storing chunks and checksums on local disks
Detecting (and recovering from) file corruption
Coordinating replica maintenance
Heartbeat messages
Basic Client implementation:

Storing files (chunk creation, determining appropriate servers)
Retrieving files in parallel
Viewing the node list, available disk space, and requests per node.
POSIX Client implementation:

Listing directories from the file system tree
Read-only file access

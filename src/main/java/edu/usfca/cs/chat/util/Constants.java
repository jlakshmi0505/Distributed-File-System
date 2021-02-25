package edu.usfca.cs.chat.util;

public class Constants {
    public static int CHUNK_SIZE = 98 * 1000 * 1000;
    public static String SALT_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    public static String CHECKSUM = "CheckSum";
    public static String FILE_TYPE = "FileType";
    public static String NUM_CHUNKS = "NumChunks";
    public static String CHUNK_ORDER = "ChunkOrder";
    public static String FILE_PATH = "FilePath";
    public static String PRIMARY = "Primary";
    public static String SECONDARY = "Secondary";
    public static String PARENT = "Parent";
    public static String META = "meta";
    public static String CHUNK_FILE_PATH = "ChunkFilePath";

    public static String DUPLICATE = "DUPLICATE";

    public static String NO_FILE = "NoFileExist";
    public static String CHECK_SUM_ERROR = "checkSumError";

    public static String WRITE = "Write";
    public static String READ = "Read";
    public static String COPY = "copy";
    public static String INFO = "info";
    public static String LS = "ls";
}

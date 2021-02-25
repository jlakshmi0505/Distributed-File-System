package edu.usfca.cs.chat.util;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static java.nio.file.StandardOpenOption.*;

public class FileUtil {
    public static long getChecksum(byte[] bytes) {
        Checksum crc32 = new CRC32();
        crc32.update(bytes, 0, bytes.length);
        return crc32.getValue();
    }

    public static boolean verifyChecksum(byte[] bytes, long stored_checksum) {
        return getChecksum(bytes) == stored_checksum;
    }

    public static void writeFileContent(String filePath, byte[] content) {
        File path = new File(filePath);
        path.getParentFile().mkdirs();
        try {
            FileOutputStream outputStream = new FileOutputStream(filePath);
            outputStream.write(content);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeMetaJsonToFile(String chunkMetaFilePath, String destFilePath, long checkSum, String filetype, int numChunks, int chunkOrder, String parent) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(Constants.CHECKSUM, checkSum);
        jsonObject.put(Constants.FILE_TYPE, filetype);
        jsonObject.put(Constants.NUM_CHUNKS, numChunks);
        jsonObject.put(Constants.CHUNK_ORDER, chunkOrder);
        jsonObject.put(Constants.CHUNK_FILE_PATH, chunkMetaFilePath.replace("_meta", ""));
        jsonObject.put(Constants.PARENT, parent);
        jsonObject.put(Constants.FILE_PATH, destFilePath);
        try {
            FileWriter file = new FileWriter(chunkMetaFilePath);
            file.write(jsonObject.toJSONString());
            file.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static byte[] readFileContent(String filePath) {
        Path file = Paths.get(filePath);
        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static JSONObject readMetaFileContent(String metaFilePath) {
        JSONParser parser = new JSONParser();
        JSONObject metaInfo = null;
        try {
            Object metaJson = parser.parse(new FileReader(metaFilePath));
            metaInfo = (JSONObject) metaJson;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return metaInfo;
    }


    public static List<JSONObject> getMetaFileChunks(String sourcePath) {
        List<JSONObject> results = new ArrayList<>();
        File dir = new File(sourcePath);
        if (!dir.exists()) {
            return results;
        }
        File[] files = dir.listFiles();
        for (File file : files) {
            if (file.getName().endsWith(Constants.META)) {
                JSONObject jsonObject = readMetaFileContent(file.getPath());
                results.add(jsonObject);
            }
        }
        return results;
    }

    public static List<JSONObject> getAllMetaFiles(String sourcePath) {
        File dir = new File(sourcePath);
        File[] files = dir.listFiles();
        List<JSONObject> results = getMetaFileChunks(dir.getPath());
        for (File file : files) {
            if (file.isDirectory()) {
                List<JSONObject> subDirChunks = getAllMetaFiles(file.getPath());
                results.addAll(subDirChunks);
            }
        }
        return results;
    }

    public static boolean andMap(boolean[] arr) {
        for (boolean b : arr) {
            if (!b) {
                return false;
            }
        }
        return true;
    }

    // https://stackoverflow.com/questions/25546750/merge-huge-files-without-loading-whole-file-into-memory
    private static void mergeFiles(List<String> fileChunks, String outputFileName) throws IOException {

        Path outFile = Paths.get(outputFileName);
        File path = new File(outputFileName);
        path.getParentFile().mkdirs();

        try (FileChannel out = FileChannel.open(outFile, CREATE, WRITE)) {
            for (String fileChunk : fileChunks) {
                Path inFile = Paths.get(fileChunk);
                try (FileChannel in = FileChannel.open(inFile, READ)) {
                    for (long p = 0, l = in.size(); p < l; )
                        p += in.transferTo(p, l - p, out);
                }
            }
        }
    }

    public static void dirToFile(String tempFilesDir, String outputFileName, String tempDir) {
        // get all file content paths
        File dir = new File(tempFilesDir);
        File[] files = dir.listFiles();
        List<String> fileChunks = new ArrayList<>();
        for (File file : files) {
            fileChunks.add(file.getPath());
        }
        try {
            Collections.sort(fileChunks);
            mergeFiles(fileChunks, outputFileName);
            cleanDirectory(tempDir, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //https://stackoverflow.com/questions/20536566/creating-a-random-string-with-a-z-and-0-9-in-java
    public static String randomString() {
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 18) { // length of the random string.
            int index = (int) (rnd.nextFloat() * Constants.SALT_CHARS.length());
            salt.append(Constants.SALT_CHARS.charAt(index));
        }
        return salt.toString();
    }

    public static String getFileName(String directory, String filePath, String chunkOrder) {
        StringJoiner stringJoiner = new StringJoiner("/");
        stringJoiner.add(directory);
        stringJoiner.add(filePath);
        stringJoiner.add(chunkOrder);
        return sanitizeFilePath(stringJoiner.toString());
    }

    public static String getFileName(String directory, String filePath) {
        StringJoiner stringJoiner = new StringJoiner("/");
        stringJoiner.add(directory);
        stringJoiner.add(filePath);
        return sanitizeFilePath(stringJoiner.toString());
    }

    public static String getMetaFileName(String fileName) {
        StringJoiner stringJoiner = new StringJoiner("_");
        stringJoiner.add(fileName);
        stringJoiner.add(Constants.META);
        return stringJoiner.toString();
    }

    public static void cleanDirectory(File dir, Boolean delDir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null && files.length > 0) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        // a sub directory, calling cleanDirectory to deleted the files
                        cleanDirectory(file, delDir);
                    }
                    file.delete();
                }
            }
            if (delDir) {
                dir.delete();
            }
        }
    }

    public static void cleanDirectory(String dir, Boolean delDir) {
        cleanDirectory(new File(dir), delDir);
    }

    public static void makeDirectory(String dir) {
        File file = new File(dir);
        file.mkdirs();
    }

    public static String sanitizeFilePath(String filePath) {
        return new File(filePath).getPath();
    }
}

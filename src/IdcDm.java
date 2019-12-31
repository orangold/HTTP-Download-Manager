import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class IdcDm {
    private static int CHUNK_SIZE = 4096;
    private static int BLOCKING_QUEUE_SIZE = 8192;
    private static String META_DATA_FILE_NAME_SUFFIX = ".chunkmap";
    private static String META_DATA_TEMP_FILE_SUFFIX = "0x0213";

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("usage: java IdcDm URL|URL-LIST-FILE [MAX-CONCURRENT-CONNECTIONS]");
            return;
        }
        var blockingQueue = new ArrayBlockingQueue<ChunkData>(BLOCKING_QUEUE_SIZE);
        var firstParam = args[0];
        var isParamURL = isURL(firstParam);
        ArrayList<String> urlsList = null;
        String currentURL;

        if (isParamURL) {
            currentURL = firstParam;
        } else {
            urlsList = getURLsFromFile(firstParam);
            if (urlsList == null) {
                return;
            }
            currentURL = getRandomURL(urlsList);
        }

        var fileSize = getFileSize(currentURL);
        if (fileSize == -1) {
            return;
        }
        System.out.println("File size " + fileSize);
        var threadCount = 1;
        if (args.length > 1) {
            threadCount = Integer.parseInt(args[1]);
        }
        var fileName = getFileName(currentURL);
        if (fileName == null) {
            return;
        }
        var randomAccessFile = getRandomAccessFile(fileName);
        if (randomAccessFile == null) {
            return;
        }

        var metaDataFileName = fileName + META_DATA_FILE_NAME_SUFFIX;
        var metaDataTempFileName = metaDataFileName + META_DATA_TEMP_FILE_SUFFIX;
        var existingChunkMap = doesChunkMapExist(metaDataFileName);
        var chunkBitMap = existingChunkMap ? getChunkMapFromFile(fileSize, metaDataFileName) : createNewChunkMap(fileSize);
        var existingChunksDataList = existingChunkMap ? generateRangeGettersList(chunkBitMap) : null;
        var totalChunksCount = existingChunkMap ? getChunksCountNeeded(existingChunksDataList) : (int) Math.ceil((double) fileSize / CHUNK_SIZE);

        startFileWriter(blockingQueue, randomAccessFile, chunkBitMap, fileName, metaDataFileName, metaDataTempFileName, totalChunksCount);
        startRangeGetters(fileSize, threadCount, currentURL, urlsList, blockingQueue, existingChunksDataList);
    }

    private static int getChunksCountNeeded(ArrayList<RangeGetterChunksData> existingChunksDataList) {
        int count = 0;
        for (int i = 0; i < existingChunksDataList.size(); i++) {
            count += existingChunksDataList.get(i).getNumOfChunks();
        }
        return count;
    }

    private static void startRangeGetters(int fileSize, int threadCount, String currentURL, ArrayList<String> urlsList, BlockingQueue blockingQueue, ArrayList<RangeGetterChunksData> existingChunksDataList) {
        if(existingChunksDataList == null || existingChunksDataList.size() == 0) {
            long currentByte = 0;
            var chunkBaseIndex = 0;
            var totalChunksCount = (int) Math.ceil((double) fileSize / CHUNK_SIZE);
            var chunksPerGetter = totalChunksCount / threadCount;
            for (int i = 0; i < threadCount; i++) {
                if (i == threadCount - 1) {
                    chunksPerGetter += totalChunksCount % threadCount;
                }
                Thread rangeGetter = new Thread(new HttpRangeGetter(currentURL, blockingQueue, chunkBaseIndex, currentByte, chunksPerGetter, CHUNK_SIZE));
                rangeGetter.start();
                currentByte += chunksPerGetter * CHUNK_SIZE;
                chunkBaseIndex += chunksPerGetter;
                if (urlsList != null) {
                    currentURL = getRandomURL(urlsList);
                }
            }
        }
        else {
//            var actualThreadCount = Math.min(threadCount, existingChunksDataList.size());
            for (int i = 0; i < threadCount; i++) {
                var getterChunkData = existingChunksDataList.get(i);
                Thread rangeGetter = new Thread(new HttpRangeGetter(currentURL, blockingQueue, getterChunkData.getStartChunkId(), getterChunkData.getStartByte(), getterChunkData.getNumOfChunks(), CHUNK_SIZE));
                rangeGetter.start();
            }
        }
    }

    private static void startFileWriter(BlockingQueue<ChunkData> queue, RandomAccessFile randomAccessFile, boolean[] chunkBitMap, String filename, String metaDataFileName, String metaDataTempFileName, int totalChunksCount) {
        Thread fileWriter = new Thread(new FileWriter(queue, randomAccessFile, chunkBitMap, filename, metaDataFileName,metaDataTempFileName, totalChunksCount));
        fileWriter.start();
    }

    private static RandomAccessFile getRandomAccessFile(String filename) {
        try {
            return new RandomAccessFile(filename, "rws");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            //TODO
        }
        return null;
    }

    // Attempts to read from existing file, if not creates a new one.
    private static boolean[] getChunkMapFromFile(int fileSize, String metaDataFileName) {
        var metaDataFile = new File(metaDataFileName);
        var totalChunksCount = (int) Math.ceil((double) fileSize / CHUNK_SIZE);
        var chunkMap = new boolean[totalChunksCount];
        try {
            var fileInputStream = new FileInputStream(metaDataFile);
            var objectInputStream = new ObjectInputStream(fileInputStream);
            chunkMap = (boolean[]) objectInputStream.readObject();
        } catch (FileNotFoundException e) {
            //TODO, PRINT TO USER
            e.printStackTrace();
            return chunkMap;
        } catch (IOException e) {
            //TODO, PRINT TO USER
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return chunkMap;
    }

    private static boolean[] createNewChunkMap(int fileSize){
        var totalChunksCount = (int) Math.ceil((double) fileSize / CHUNK_SIZE);
        return new boolean[totalChunksCount];
    }

    private static boolean doesChunkMapExist(String metaDataFileName){
        var metaDataFile = new File(metaDataFileName);
        return metaDataFile.exists();
    }

    private static ArrayList<RangeGetterChunksData> generateRangeGettersList(boolean[] chunkMap) {
        var list = new ArrayList<RangeGetterChunksData>();
        var currentSequentialCount = 0;
        var currentStartChunkId = 0;
        for (int i = 0; i < chunkMap.length; i++) {
            if (chunkMap[i]) {
                if (currentSequentialCount > 0) {
                    var rangeGetterChunkData = new RangeGetterChunksData(currentStartChunkId * CHUNK_SIZE, currentSequentialCount, currentStartChunkId);
                    list.add(rangeGetterChunkData);
                    currentSequentialCount = 0;
                }
            } else {
                if (currentSequentialCount == 0) {
                    currentStartChunkId = i;
                }
                currentSequentialCount++;
            }
        }
        var reachedEndWithLeftOvers = currentSequentialCount != 0;
        if (reachedEndWithLeftOvers) {
            var rangeGetterChunkData = new RangeGetterChunksData(currentStartChunkId * CHUNK_SIZE, currentSequentialCount, currentStartChunkId);
            list.add(rangeGetterChunkData);
        }
        return list;
    }


    //TODO: move to Utils

    private static ArrayList<String> getURLsFromFile(String fileName) {
        var urlList = new ArrayList<String>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                urlList.add(line);
            }
        } catch (FileNotFoundException e) {
            System.err.println("URL file list not found!");
            return null;
        } catch (IOException e) {
            System.err.println("Failed to read URLs from file");
            return null;
        }
        if (urlList.size() == 0) {
            System.err.println("No URLs found in file");
            return null;
        }
        return urlList;
    }

    private static boolean isURL(String connString) {
        try {
            new URL(connString);
            return true;
        } catch (MalformedURLException e) {
            return false;
        }
    }

    private static String getFileName(String connString) {
        try {
            var url = new URL(connString);
            String urlPath = url.getPath();
            return urlPath.substring(urlPath.lastIndexOf('/') + 1);
        } catch (MalformedURLException ex) {
            //todo
        }
        return null;
    }

    private static int getFileSize(String connString) {
        try {
            var url = new URL(connString);
            var urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.connect();
            return urlConnection.getContentLength();
        } catch (MalformedURLException malformedEx) {
            System.err.println("Failed to parse URL, please make sure the URL is in a correct format");
        } catch (IOException ioEx) {
            System.err.println("Failed to fetch file from URL, make sure the URL is correct");
        }
        return -1;
    }

    private static String getRandomURL(ArrayList<String> URLs) {
        var rand = new Random();
        return URLs.get(rand.nextInt(URLs.size()));
    }
}

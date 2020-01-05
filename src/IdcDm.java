import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class IdcDm {
    //TODO: what if one thread finishes before the rest and then we stop ?
    //TODO: test mirroring and url file
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("usage: java IdcDm URL|URL-LIST-FILE [MAX-CONCURRENT-CONNECTIONS]");
            return;
        }
        var blockingQueue = new ArrayBlockingQueue<FileWriterChunkData>(Consts.BLOCKING_QUEUE_SIZE);
        var firstParam = args[0];
        var isParamURL = Utils.isURL(firstParam);
        ArrayList<String> urlsList = null;
        String currentURL;

        if (isParamURL) {
            currentURL = firstParam;
        } else {
            urlsList = Utils.getURLsFromFile(firstParam);
            if (urlsList == null) {
                return;
            }
            currentURL = Utils.getRandomURL(urlsList);
        }

        var fileSize = Utils.getFileSize(currentURL);
        if (fileSize == -1) {
            return;
        }

        var threadCount = 1;
        if (args.length > 1) {
            threadCount = Integer.parseInt(args[1]);
        }
        var fileName = Utils.getFileName(currentURL);
        if (fileName == null) {
            return;
        }
        var randomAccessFile = createDownloadFile(fileName);
        if (randomAccessFile == null) {
            return;
        }

        var metaDataFileName = fileName + Consts.META_DATA_FILE_NAME_SUFFIX;
        var metaDataTempFileName = metaDataFileName + Consts.META_DATA_TEMP_FILE_SUFFIX;
        var existingChunkMap = doesChunkMapExist(metaDataFileName);
        var chunkBitMap = existingChunkMap ? getChunkMapFromFile(fileSize, metaDataFileName) : createNewChunkMap(fileSize);
        var existingChunksDataList = existingChunkMap ? generateRangeGettersList(chunkBitMap) : null;
        var totalChunks = (int) Math.ceil((double) fileSize / Consts.CHUNK_SIZE);
        var totalChunksToDownload = existingChunkMap ? getChunksCountNeeded(existingChunksDataList) : totalChunks;
        startFileWriter(blockingQueue, randomAccessFile, chunkBitMap, fileName, metaDataFileName, metaDataTempFileName, totalChunks, totalChunksToDownload);
        startRangeGetters(fileSize, threadCount, currentURL, urlsList, blockingQueue, existingChunksDataList);
    }

    private static void startFileWriter(BlockingQueue<FileWriterChunkData> queue, RandomAccessFile randomAccessFile, boolean[] chunkBitMap, String filename, String metaDataFileName, String metaDataTempFileName, int totalChunks, int totalChunksToDownload) {
        Thread fileWriter = new Thread(new FileWriter(queue, randomAccessFile, chunkBitMap, filename, metaDataFileName, metaDataTempFileName, totalChunks, totalChunksToDownload));
        fileWriter.start();
    }

    private static void startRangeGetters(int fileSize, int threadCount, String currentURL, ArrayList<String> urlsList, BlockingQueue blockingQueue, ArrayList<RangeGetterChunksData> existingChunksDataList) {
        if (existingChunksDataList == null || existingChunksDataList.size() == 0) {
            startNewDownload(threadCount, fileSize, currentURL, urlsList, blockingQueue);
        } else {
            var noNeedForQueuing = threadCount >= existingChunksDataList.size();
            if (noNeedForQueuing) {
                resumeExistingDownload(existingChunksDataList, currentURL, urlsList, blockingQueue);
            } else {
                resumeExistingDownloadWithQueuing(threadCount, existingChunksDataList, currentURL, urlsList, blockingQueue);
            }
        }
    }

    private static void startNewDownload(int threadCount, int fileSize, String currentURL, ArrayList<String> urlsList, BlockingQueue blockingQueue) {
        printDownloading(threadCount);
        long currentByte = 0;
        var chunkBaseIndex = 0;
        var totalChunksCount = (int) Math.ceil((double) fileSize / Consts.CHUNK_SIZE);
        var chunksPerGetter = totalChunksCount / threadCount;
        for (int i = 0; i < threadCount; i++) {
            if (i == threadCount - 1) {
                chunksPerGetter += totalChunksCount % threadCount;
            }
            var rangeGetterChunkData = new RangeGetterChunksData(currentByte, chunksPerGetter, chunkBaseIndex);
            var singleRangeQueue = new ArrayDeque<RangeGetterChunksData>();
            singleRangeQueue.add(rangeGetterChunkData);
            var rangeGetterThread = new Thread(new HttpRangeGetter(currentURL, blockingQueue, Consts.CHUNK_SIZE, Consts.RANGE_GETTERS_INITITAL_ID + i, singleRangeQueue));
            rangeGetterThread.start();
            currentByte += chunksPerGetter * Consts.CHUNK_SIZE;
            chunkBaseIndex += chunksPerGetter;
            if (urlsList != null) {
                currentURL = Utils.getRandomURL(urlsList);
            }
        }
    }

    private static void resumeExistingDownload(ArrayList<RangeGetterChunksData> existingChunksDataList, String currentURL, ArrayList<String> urlsList, BlockingQueue blockingQueue) {
        var threadCount = existingChunksDataList.size();
        printDownloading(threadCount);
        for (int i = 0; i < threadCount; i++) {
            var rangeGetterChunkData = existingChunksDataList.get(i);
            var singleRangeQueue = new ArrayDeque<RangeGetterChunksData>();
            singleRangeQueue.add(rangeGetterChunkData);
            Thread rangeGetter = new Thread(new HttpRangeGetter(currentURL, blockingQueue, Consts.CHUNK_SIZE, Consts.RANGE_GETTERS_INITITAL_ID + i, singleRangeQueue));
            rangeGetter.start();
            if (urlsList != null) {
                currentURL = Utils.getRandomURL(urlsList);
            }
        }
    }

    private static void resumeExistingDownloadWithQueuing(int threadCount, ArrayList<RangeGetterChunksData> existingChunksDataList, String currentURL, ArrayList<String> urlsList, BlockingQueue blockingQueue) {
        printDownloading(threadCount);
        var chunkBatchesPerThread = existingChunksDataList.size() / threadCount;
        var remainderBatchesToQueue = existingChunksDataList.size() % threadCount;
        var additionalPerRangeGetter = (int) Math.ceil((double) remainderBatchesToQueue / threadCount);
        for (int i = 0; i < threadCount; i++) {
            var rangesQueue = new ArrayDeque<RangeGetterChunksData>();
            for (int j = 0; j < chunkBatchesPerThread; j++) {
                rangesQueue.add(existingChunksDataList.remove(0));
            }
            for (int j = 0; j < additionalPerRangeGetter && remainderBatchesToQueue > 0; j++) {
                rangesQueue.add(existingChunksDataList.remove(0));
                remainderBatchesToQueue--;
            }
            Thread rangeGetter = new Thread(new HttpRangeGetter(currentURL, blockingQueue, Consts.CHUNK_SIZE, Consts.RANGE_GETTERS_INITITAL_ID + i, rangesQueue));
            rangeGetter.start();
            if (urlsList != null) {
                currentURL = Utils.getRandomURL(urlsList);
            }
        }
    }

    private static ArrayList<RangeGetterChunksData> generateRangeGettersList(boolean[] chunkMap) {
        var list = new ArrayList<RangeGetterChunksData>();
        var currentSequentialCount = 0;
        var currentStartChunkId = 0;
        for (int i = 0; i < chunkMap.length; i++) {
            if (chunkMap[i]) {
                if (currentSequentialCount > 0) {
                    var rangeGetterChunkData = new RangeGetterChunksData(currentStartChunkId * Consts.CHUNK_SIZE, currentSequentialCount, currentStartChunkId);
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
            var rangeGetterChunkData = new RangeGetterChunksData(currentStartChunkId * Consts.CHUNK_SIZE, currentSequentialCount, currentStartChunkId);
            list.add(rangeGetterChunkData);
        }
        return list;
    }

    private static RandomAccessFile createDownloadFile(String filename) {
        try {
            return new RandomAccessFile(filename, "rws");
        } catch (FileNotFoundException e) {
            Utils.printErrorMessageWithFailure("Failed to create download file");
        }
        return null;
    }

    private static void printDownloading(int threadCount) {
        if (threadCount == 1) {
            System.out.println("Downloading...");
            return;
        }
        System.out.println("Downloading using " + threadCount + " connections...");
    }

    private static int getChunksCountNeeded(ArrayList<RangeGetterChunksData> existingChunksDataList) {
        var count = 0;
        for (int i = 0; i < existingChunksDataList.size(); i++) {
            count += existingChunksDataList.get(i).getNumOfChunks();
        }
        return count;
    }

    // Attempts to read from existing file, if not creates a new one.
    private static boolean[] getChunkMapFromFile(int fileSize, String metaDataFileName) {
        var metaDataFile = new File(metaDataFileName);
        var totalChunksCount = (int) Math.ceil((double) fileSize / Consts.CHUNK_SIZE);
        var chunkMap = new boolean[totalChunksCount];
        try (var fileInputStream = new FileInputStream(metaDataFile);
             var objectInputStream = new ObjectInputStream(fileInputStream)) {
            chunkMap = (boolean[]) objectInputStream.readObject();
        } catch (Exception e) { // Same behaviour for all exceptions, delete the chunk file and restart
            Utils.printErrorMessage("Failed to fetch existing file data, restarting download..");
            var success = metaDataFile.delete();
            if (!success) {
                Utils.printErrorMessage("Please make sure meta data file is not being used by another process");
            }
            return new boolean[totalChunksCount];
        }
        return chunkMap;
    }

    private static boolean[] createNewChunkMap(int fileSize) {
        var totalChunksCount = (int) Math.ceil((double) fileSize / Consts.CHUNK_SIZE);
        return new boolean[totalChunksCount];
    }

    private static boolean doesChunkMapExist(String metaDataFileName) {
        var metaDataFile = new File(metaDataFileName);
        return metaDataFile.exists();
    }
}

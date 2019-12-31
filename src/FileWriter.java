import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.BlockingQueue;

public class FileWriter implements Runnable {
    private static int SAVE_METADATA_PER_CHUNKS_COUNT = 32;

    private BlockingQueue<FileWriterChunkData> blockingQueue;
    private boolean[] chunkBitMap;
    private String fileName, metaDataFileName, metaDataTempFileName;
    private RandomAccessFile randomAccessFile;
    private int totalChunks, totalChunksNeededDownload;
    private int currentProgress = 0;

    public FileWriter(BlockingQueue<FileWriterChunkData> blockingQueue, RandomAccessFile randomAccessFile, boolean[] chunkBitMap, String fileName, String metaDataFileName, String metaDataTempFileName, int totalChunks, int totalChunksNeededDownload) {
        this.blockingQueue = blockingQueue;
        this.randomAccessFile = randomAccessFile;
        this.chunkBitMap = chunkBitMap;
        this.fileName = fileName;
        this.metaDataFileName = metaDataFileName;
        this.metaDataTempFileName = metaDataTempFileName;
        this.totalChunks = totalChunks;
        this.totalChunksNeededDownload = totalChunksNeededDownload;
    }

    @Override
    public void run() {
        try {
            var chunksRead = this.totalChunks - this.totalChunksNeededDownload;
            while (true) {
                var chunk = this.blockingQueue.take();
                chunksRead++;
                printProgress(chunksRead);
                saveChunkToChunkMap(chunk);
                updateChunkMap(chunk);
                var saveMetadataToDisc = chunksRead % SAVE_METADATA_PER_CHUNKS_COUNT == SAVE_METADATA_PER_CHUNKS_COUNT - 1;
                if (saveMetadataToDisc) {
                    saveChunkBitMapToFile();
                }
                if (chunksRead == this.totalChunks) {
                    onDownloadFinished();
                    break;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            //TODO
        }
    }

    private void printProgress(int chunksRead) {
        var newProgress = (int)(100 * ((double)chunksRead / this.totalChunks));
        if(newProgress != this.currentProgress){
            System.out.println("Downloaded " +newProgress+"%");
            this.currentProgress=newProgress;
        }
    }

    private void saveChunkBitMapToFile() {
        //TODO: try with resources?
        try {
            var metaDataFile = new File(this.metaDataFileName);
            var tempMetaDataFile = new File(this.metaDataTempFileName);
            var fileOutputStream = new FileOutputStream(tempMetaDataFile);
            var objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(this.chunkBitMap);
            fileOutputStream.close();
            objectOutputStream.close();
            Files.move(tempMetaDataFile.toPath(), metaDataFile.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            e.printStackTrace();
            //TODO close?
        }
    }

    private void onDownloadFinished() {
        System.out.println("Download succeeded");
        deleteTempFiles();
    }

    private void updateChunkMap(FileWriterChunkData fileWriterChunkData) {
        this.chunkBitMap[fileWriterChunkData.getChunkId()] = true;
    }

    private void saveChunkToChunkMap(FileWriterChunkData fileWriterChunkData) {
        try {
            this.randomAccessFile.seek(fileWriterChunkData.getStartByte());
            this.randomAccessFile.write(fileWriterChunkData.getData(), 0, fileWriterChunkData.getLength());
//            System.out.println("saved chunk id "+chunkData.getChunkId());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deleteTempFiles() {
        var metaDataFile = new File(this.metaDataFileName);
        var tempMetaDataFile = new File(this.metaDataTempFileName);
        if (tempMetaDataFile.exists()) {
            var tempMetaDataDeleted = tempMetaDataFile.delete();
            if (!tempMetaDataDeleted) {
                System.out.println("Failed temp file delete!");
                //TODO
            }
        }
        var metaDataDeleted = metaDataFile.delete();
        if (!metaDataDeleted) {
            System.out.println("Failed meta file delete!");
            //TODO
        }

    }
}

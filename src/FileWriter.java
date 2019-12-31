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
    private int totalChunksNeededDownload;

    public FileWriter(BlockingQueue<FileWriterChunkData> blockingQueue, RandomAccessFile randomAccessFile, boolean[] chunkBitMap, String fileName, String metaDataFileName, String metaDataTempFileName, int totalChunksNeededDownload) {
        this.blockingQueue = blockingQueue;
        this.randomAccessFile = randomAccessFile;
        this.chunkBitMap = chunkBitMap;
        this.fileName = fileName;
        this.metaDataFileName = metaDataFileName;
        this.metaDataTempFileName = metaDataTempFileName;
        this.totalChunksNeededDownload = totalChunksNeededDownload;
    }

    @Override
    public void run() {
        try {
            var chunksRead = 0;
            while (true) {
                var chunk = this.blockingQueue.take();
                chunksRead++;
                saveChunkToChunkMap(chunk);
                updateChunkMap(chunk);
                var saveMetadataToDisc = chunksRead % SAVE_METADATA_PER_CHUNKS_COUNT == SAVE_METADATA_PER_CHUNKS_COUNT - 1;
                if (saveMetadataToDisc) {
                    saveChunkBitMapToFile();
                }
                if (chunksRead == this.totalChunksNeededDownload) {
                    deleteTempFiles();
                    break;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            //TODO
        }
    }

    private void saveChunkBitMapToFile() {
        //TODO: try with resources?
        try {
//            System.out.println("Saving chunk map..");
            var metaDataFile = new File(this.metaDataFileName);
            var tempMetaDataFile = new File(this.metaDataTempFileName);
            var fileOutputStream = new FileOutputStream(tempMetaDataFile);
            var objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(this.chunkBitMap);
            fileOutputStream.close();
            objectOutputStream.close();
            //TODO: is it okay to use nio
            Files.move(tempMetaDataFile.toPath(), metaDataFile.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            e.printStackTrace();
            //TODO close?
        }
    }

    private void updateChunkMap(FileWriterChunkData fileWriterChunkData){
        this.chunkBitMap[fileWriterChunkData.getChunkId()] = true;
    }

    private void saveChunkToChunkMap(FileWriterChunkData fileWriterChunkData){
        try {
            this.randomAccessFile.seek(fileWriterChunkData.getStartByte());
            this.randomAccessFile.write(fileWriterChunkData.getData(),0, fileWriterChunkData.getLength());
//            System.out.println("saved chunk id "+chunkData.getChunkId());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deleteTempFiles() {
        System.out.println("Deleting metadata..");
        var metaDataFile = new File(this.metaDataFileName);
        var tempMetaDataFile = new File(this.metaDataTempFileName);
        if(tempMetaDataFile.exists()){
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

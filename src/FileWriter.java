import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.BlockingQueue;

public class FileWriter implements Runnable {
    private static int SAVE_METADATA_PER_CHUNKS_COUNT = 128;

    private BlockingQueue<ChunkData> blockingQueue;
    private boolean[] chunkBitMap;
    private String fileName, metaDataFileName, metaDataTempFileName;
    private RandomAccessFile randomAccessFile;
    private int totalChunks;

    public FileWriter(BlockingQueue<ChunkData> blockingQueue,RandomAccessFile randomAccessFile, boolean[] chunkBitMap, String fileName, String metaDataFileName, String metaDataTempFileName, int totalChunks) {
        this.blockingQueue = blockingQueue;
        this.randomAccessFile = randomAccessFile;
        this.chunkBitMap = chunkBitMap;
        this.fileName = fileName;
        this.metaDataFileName = metaDataFileName;
        this.metaDataTempFileName = metaDataTempFileName;
        this.totalChunks = totalChunks;
    }

    @Override
    public void run() {
        try {
            var chunksRead = 0;
            while (true) {
                var chunk = this.blockingQueue.take();
                chunksRead++;
                saveChunkToFile(chunk);
                updateChunkMap(chunk);
                var saveMetadataToDisc = chunksRead % SAVE_METADATA_PER_CHUNKS_COUNT == SAVE_METADATA_PER_CHUNKS_COUNT - 1;
                if (saveMetadataToDisc) {
                    saveChunkBitMap();
                }
                if (chunksRead == this.totalChunks) {
                    deleteTempFiles();
                    break;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            //TODO
        }
    }

    private void saveChunkBitMap() {
        //TODO: try with resources?
        try {
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

    private void updateChunkMap(ChunkData chunkData){
        this.chunkBitMap[chunkData.getChunkId()] = true;
    }

    private void saveChunkToFile(ChunkData chunkData){
        try {
            this.randomAccessFile.seek(chunkData.getStartByte());
            this.randomAccessFile.write(chunkData.getData(),0,chunkData.getLength());
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

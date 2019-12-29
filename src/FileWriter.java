import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.BlockingQueue;

public class FileWriter implements Runnable {
    private static String METADATA_FILE_BEFORE_RENAME_SUFFIX = "0x0213";
//    private static int SAVE_METADATA_CHUNKS_COUNT = 1024;

    private BlockingQueue<ChunkData> blockingQueue;
    private boolean[] chunkBitMap;
    private String fileName, metaDataFileNameSuffix;
    private RandomAccessFile randomAccessFile;

    public FileWriter(BlockingQueue<ChunkData> blockingQueue,RandomAccessFile randomAccessFile, boolean[] chunkBitMap, String fileName, String metaDataFileNameSuffix) {
        this.blockingQueue = blockingQueue;
        this.randomAccessFile =randomAccessFile;
        this.chunkBitMap = chunkBitMap;
        this.fileName = fileName;
        this.metaDataFileNameSuffix = metaDataFileNameSuffix;
    }

    @Override
    public void run() {
        try {
//            var chunksRead = 0;
            while (true) {
                var chunk = this.blockingQueue.take();
//                chunksRead++;
                saveChunkToFile(chunk);
                updateChunkMap(chunk);
//                var saveMetadataToDisc = chunksRead % SAVE_METADATA_CHUNKS_COUNT == SAVE_METADATA_CHUNKS_COUNT - 1;
//                if (saveMetadataToDisc) {
                    saveChunkBitMap();
//                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            //TODO
        }
    }

    private void saveChunkBitMap() {
        //TODO: try with resources?
        try {
            var metaDataName = this.fileName + this.metaDataFileNameSuffix;
            var metaDataFile = new File(metaDataName);
            var tempMetaDataFile = new File(metaDataName + METADATA_FILE_BEFORE_RENAME_SUFFIX);
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
}

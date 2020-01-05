import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.BlockingQueue;

public class FileWriter implements Runnable {
    private BlockingQueue<FileWriterChunkData> blockingQueue;
    private boolean[] chunkBitMap;
    private String fileName, metaDataFileName, metaDataTempFileName;
    private RandomAccessFile randomAccessFile;
    private int totalChunks, totalChunksNeededDownload;
    private int currentProgress = 0;

    private File metaDataFile, tempMetaDataFile;

    public FileWriter(BlockingQueue<FileWriterChunkData> blockingQueue, RandomAccessFile randomAccessFile, boolean[] chunkBitMap, String fileName, String metaDataFileName, String metaDataTempFileName, int totalChunks, int totalChunksNeededDownload) {
        this.blockingQueue = blockingQueue;
        this.randomAccessFile = randomAccessFile;
        this.chunkBitMap = chunkBitMap;
        this.fileName = fileName;
        this.metaDataFileName = metaDataFileName;
        this.metaDataTempFileName = metaDataTempFileName;
        this.totalChunks = totalChunks;
        this.totalChunksNeededDownload = totalChunksNeededDownload;

        this.metaDataFile = new File(this.metaDataFileName);
        this.tempMetaDataFile = new File(this.metaDataTempFileName);
    }

    @Override
    public void run() {
        try {
            var chunksRead = this.totalChunks - this.totalChunksNeededDownload;
            while (true) {
                var chunk = this.blockingQueue.take();
                var rangeGetterCrashed = chunk.isEmpty();
                if (rangeGetterCrashed) {
                    Utils.printErrorMessageWithFailure("One of the Downloaders crashed, terminating..");
                    return;
                }
                chunksRead++;
                saveChunkToDownloadFile(chunk);
                updateChunkMap(chunk);
                var saveMetadataToDisc = chunksRead % Consts.SAVE_METADATA_PER_CHUNKS_COUNT == Consts.SAVE_METADATA_PER_CHUNKS_COUNT - 1;
                if (saveMetadataToDisc) {
                    saveChunkBitMapToFile();
                }
                if (chunksRead == this.totalChunks) {
                    onDownloadFinished();
                    break;
                }
                printProgress(chunksRead);
            }
        } catch (InterruptedException e) {
            Utils.printErrorMessageWithFailure("Download writing interrupted, please restart download manager");
        }
    }

    private void printProgress(int chunksRead) {
        var newProgress = (int) (100 * ((double) chunksRead / this.totalChunks));
        if (newProgress != this.currentProgress) {
            System.out.println("Downloaded " + newProgress + "%");
            this.currentProgress = newProgress;
        }
    }

    private void saveChunkBitMapToFile() {
        //TODO: maybe keep this open at all times, only close at the end..?
        try (var fileOutputStream = new FileOutputStream(tempMetaDataFile);
             var objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
            objectOutputStream.writeObject(this.chunkBitMap);
            Files.copy(this.tempMetaDataFile.toPath(), this.metaDataFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            //TODO
            System.out.println(e.getMessage());
            Utils.printErrorMessage("Failed to save download metadata, retrying..");
        }
    }

    private void onDownloadFinished() {
        System.out.println("Download succeeded");
        try {
            this.randomAccessFile.close();
        } catch (IOException e) {
            Utils.printErrorMessage("Failed to close connection with the file downloaded.\nIf you cannot access the file, wait a few minutes or restart your computer");
        }
        deleteTempMetadataFiles();
    }

    private void updateChunkMap(FileWriterChunkData fileWriterChunkData) {
        this.chunkBitMap[fileWriterChunkData.getChunkId()] = true;
    }

    private void saveChunkToDownloadFile(FileWriterChunkData fileWriterChunkData) {
        try {
            this.randomAccessFile.seek(fileWriterChunkData.getStartByte());
            this.randomAccessFile.write(fileWriterChunkData.getData(), 0, fileWriterChunkData.getLength());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deleteTempMetadataFiles() {
        if (this.tempMetaDataFile.exists()) {
            var tempMetaDataDeleted = this.tempMetaDataFile.delete();
            if (!tempMetaDataDeleted) {
                Utils.printErrorMessage("Failed to delete temp file");
            }
        }
        if (this.metaDataFile.exists()) {
            var metaDataDeleted = this.metaDataFile.delete();
            if (!metaDataDeleted) {
                Utils.printErrorMessage("Failed to delete meta file");
            }
        }
    }
}

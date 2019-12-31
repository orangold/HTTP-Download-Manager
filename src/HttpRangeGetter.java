import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

public class HttpRangeGetter implements Runnable {
    private String connectionString;
    private BlockingQueue<FileWriterChunkData> blockingQueue;
    private int chunkSize, Id;
    private Queue<RangeGetterChunksData> rangeToDownloadQueue;

    public HttpRangeGetter(String connectionString, BlockingQueue<FileWriterChunkData> blockingQueue, int chunkSize, int getterId, Queue<RangeGetterChunksData> rangeGetterChunksData) {
        this.connectionString = connectionString;
        this.blockingQueue = blockingQueue;
        this.rangeToDownloadQueue = rangeGetterChunksData;
        this.chunkSize = chunkSize;
        this.Id = getterId;
    }

    @Override
    public void run() {
        RangeGetterChunksData rangeGetterData;
        while ((rangeGetterData = rangeToDownloadQueue.poll()) != null) {
            BufferedInputStream inputStream = setupConnection(rangeGetterData);
            var currentChunkIndex = rangeGetterData.getStartChunkId();
            if (inputStream == null) {
                return;
            }
            int nextInt;
            byte nextByte;
            long currentByte = rangeGetterData.getStartByte();
            var bufferCurrentIndex = 0;
            var readBuffer = new byte[this.chunkSize];
            try {
                while ((nextInt = inputStream.read()) != -1) {
                    nextByte = (byte) nextInt;
                    if (bufferCurrentIndex < readBuffer.length - 1) {
                        readBuffer[bufferCurrentIndex] = nextByte;
                        bufferCurrentIndex++;
                    } else {
//                    System.out.println(String.format("Downloaded %d to %d", currentByte, currentByte + bufferCurrentIndex - 1));
                        readBuffer[bufferCurrentIndex] = nextByte;
                        var chuckData = createChunkData(currentByte, bufferCurrentIndex + 1, currentChunkIndex, readBuffer.clone());
                        blockingQueue.put(chuckData);
                        currentByte += bufferCurrentIndex + 1;
                        bufferCurrentIndex = 0;
                        currentChunkIndex++;
                    }
                }
                var leftOvers = bufferCurrentIndex != 0;
                if (leftOvers) {
                    var chuckData = createChunkData(currentByte, bufferCurrentIndex, currentChunkIndex, readBuffer.clone());
                    blockingQueue.put(chuckData);
                    currentByte += bufferCurrentIndex;
                    bufferCurrentIndex = 0;
                    currentChunkIndex++;
                }
//                [15] Finished downloading
                System.out.printf("[%d] Finished downloading\n", this.Id);
//                System.out.println("Done!");
            } catch (IOException ioEx) {
                // TODO
            } catch (InterruptedException e) {
                // TODO
            }
        }
    }

    private BufferedInputStream setupConnection(RangeGetterChunksData rangeGetterChunksData) {
        try {
            // NOTE: can request out of range and still be okay ! yay!
            var url = new URL(this.connectionString);
            var urlConnection = (HttpURLConnection) url.openConnection();
            var endByte = rangeGetterChunksData.getStartByte() + rangeGetterChunksData.getNumOfChunks() * this.chunkSize - 1;
            urlConnection.setRequestProperty("Range", String.format("bytes=%d-%d", rangeGetterChunksData.getStartByte(), endByte));
//            [13] Start downloading range (0 - 240123903) from:
//            http://centos.activecloud.co.il/6.10/isos/x86_64/CentOS-6.10-x86_64-netinstall.iso
//            System.out.println("Requesting " + rangeGetterChunksData.getStartByte() + " to " + endByte);
            System.out.printf("[%d] Start downloading range (%d - %d) from:\n%s\n",
                    this.Id, rangeGetterChunksData.getStartByte(), endByte, this.connectionString);
            urlConnection.connect();
            return new BufferedInputStream(urlConnection.getInputStream());
        } catch (MalformedURLException malformedEx) {
            System.out.println(malformedEx.getMessage());
            // TODO
        } catch (IOException ioEx) {
            System.out.println(ioEx.getMessage());
            // TODO
        }
        return null;
    }

    private FileWriterChunkData createChunkData(long currentByte, int dataLength, int chunkId, byte[] buffer) {
        return new FileWriterChunkData(currentByte, dataLength, chunkId, buffer);
    }
}

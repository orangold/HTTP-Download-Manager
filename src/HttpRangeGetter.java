import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.BlockingQueue;

public class HttpRangeGetter implements Runnable {
    private String connectionString;
    private BlockingQueue<ChunkData> blockingQueue;
    private int numOfChunks, chunkSize, chunkIndex;
    private long startByte;

    public HttpRangeGetter(String connectionString, BlockingQueue<ChunkData> blockingQueue, int chunkIndex, long startByte, int numOfChunks, int chunkSize) {
        this.connectionString = connectionString;
        this.blockingQueue = blockingQueue;
        this.startByte = startByte;
        this.numOfChunks = numOfChunks;
        this.chunkSize = chunkSize;
        this.chunkIndex = chunkIndex;
    }

    @Override
    public void run() {
        BufferedInputStream inputStream = setupConnection();
        if (inputStream == null) {
            return;
        }
        int nextInt;
        byte nextByte;
        var currentByte = startByte;
        var currentIndex = 0;
        var readBuffer = new byte[this.chunkSize];
        try {
            while ((nextInt = inputStream.read()) != -1) {
                nextByte = (byte) nextInt;
                if (currentIndex < readBuffer.length) {
                    readBuffer[currentIndex] = nextByte;
                    currentIndex++;
                } else {
//                    System.out.println(String.format("Downloaded %d to %d", currentByte, currentByte + currentIndex - 1));
                    var chuckData = createChunkData(currentByte, currentIndex, this.chunkIndex, readBuffer);
                    blockingQueue.put(chuckData);
                    currentByte += currentIndex;
                    currentIndex = 0;
                    this.chunkIndex++;
                }
            }
            var leftOvers = currentIndex != 0;
            if (leftOvers) {
                var chuckData = createChunkData(currentByte, currentIndex + 1, this.chunkIndex, readBuffer);
                blockingQueue.put(chuckData);
                currentByte += currentIndex + 1;
                currentIndex = 0;
                this.chunkIndex++;
            }

            System.out.println("Done!");
        } catch (IOException ioEx) {
            // TODO
        } catch (InterruptedException e) {
            // TODO
        }
    }

    private BufferedInputStream setupConnection() {
        try {
            var url = new URL(this.connectionString);
            var urlConnection = (HttpURLConnection) url.openConnection();
            var endByte = this.startByte + this.numOfChunks * this.chunkSize - 1;
            urlConnection.setRequestProperty("Range", String.format("bytes=%d-%d", this.startByte, endByte));
            System.out.println("Requesting " + this.startByte + " to " + endByte);
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

    private ChunkData createChunkData(long currentByte, int dataLength, int chunkId, byte[] buffer) {
        return new ChunkData(currentByte, dataLength, chunkId, buffer);
    }
}

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
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
            BufferedInputStream inputStream = setupConnection(rangeGetterData, 0);
            var currentChunkIndex = rangeGetterData.getStartChunkId();
            try {
                if (inputStream == null) {
                    notifyWriterOfCrashing();
                    return;
                }
                int nextInt;
                byte nextByte;
                long currentByte = rangeGetterData.getStartByte();
                var bufferCurrentIndex = 0;
                var readBuffer = new byte[this.chunkSize];
                while ((nextInt = inputStream.read()) != -1) {
                    nextByte = (byte) nextInt;
                    if (bufferCurrentIndex < readBuffer.length - 1) {
                        readBuffer[bufferCurrentIndex] = nextByte;
                        bufferCurrentIndex++;
                    } else {
                        readBuffer[bufferCurrentIndex] = nextByte;
                        var chuckData = createChunkData(currentByte, bufferCurrentIndex + 1, currentChunkIndex, readBuffer.clone());
                        this.blockingQueue.put(chuckData);
                        currentByte += bufferCurrentIndex + 1;
                        bufferCurrentIndex = 0;
                        currentChunkIndex++;
                    }
                }
                var leftOvers = bufferCurrentIndex != 0;
                if (leftOvers) {
                    var chuckData = createChunkData(currentByte, bufferCurrentIndex, currentChunkIndex, readBuffer.clone());
                    this.blockingQueue.put(chuckData);
                    currentByte += bufferCurrentIndex;
                    bufferCurrentIndex = 0;
                    currentChunkIndex++;
                }
                System.out.printf("[%d] Finished downloading\n", this.Id);
                return;
            } catch (SocketTimeoutException ex) {
                Utils.printErrorMessage(String.format("[%d] Received timeout from server, terminating..", this.Id));
            } catch (IOException ioEx) {
                Utils.printErrorMessage(String.format("[%d] Connection lost, terminating..", this.Id));
            } catch (InterruptedException e) {
                Utils.printErrorMessage(String.format("[%d] Interrupted, please restart the program..", this.Id));
            }
            notifyWriterOfCrashing();
        }
    }

    private BufferedInputStream setupConnection(RangeGetterChunksData rangeGetterChunksData, int currentAttempt) {
        try {
            var url = new URL(this.connectionString);
            var urlConnection = (HttpURLConnection) url.openConnection();
//            urlConnection.setConnectTimeout(Consts.SETUP_CONNECTION_TIMEOUT_MSECONDS);
//            urlConnection.setReadTimeout(Consts.READ_TIMEOUT_MSECONDS);
            var endByte = rangeGetterChunksData.getStartByte() + rangeGetterChunksData.getNumOfChunks() * this.chunkSize - 1;
            urlConnection.setRequestProperty("Range", String.format("bytes=%d-%d", rangeGetterChunksData.getStartByte(), endByte));
            System.out.printf("[%d] Start downloading range (%d - %d) from:\n%s\n",
                    this.Id, rangeGetterChunksData.getStartByte(), endByte, this.connectionString);
            urlConnection.connect();
            return new BufferedInputStream(urlConnection.getInputStream());
        } catch (MalformedURLException malformedEx) {
            Utils.printErrorMessage("Failed to parse URL, please make sure the URL is in a correct format");
            return null;
        } catch (IOException ioEx) {
            if (currentAttempt > Consts.MAX_CONNECTION_RETRIES - 1) {
                Utils.printErrorMessage(String.format("[%d] Failed to setup connection after %d retries", this.Id, Consts.MAX_CONNECTION_RETRIES));
                return null;
            }
            Utils.printErrorMessage(String.format("[%d] Failed to setup connection. Retrying..\n Retries left: %d", this.Id, Consts.MAX_CONNECTION_RETRIES - currentAttempt));
            return setupConnection(rangeGetterChunksData, ++currentAttempt);
        }
    }

    private FileWriterChunkData createChunkData(long currentByte, int dataLength, int chunkId, byte[] buffer) {
        return new FileWriterChunkData(currentByte, dataLength, chunkId, buffer);
    }

    private void notifyWriterOfCrashing() {
        var crashedChunkIndicator = createEmptyChunk();
        try {
            this.blockingQueue.put(crashedChunkIndicator);
        } catch (InterruptedException e) {
            Utils.printErrorMessage(String.format("[%d] Interrupted, please restart the program..", this.Id));
        }
    }

    private FileWriterChunkData createEmptyChunk() {
        return new FileWriterChunkData();
    }
}

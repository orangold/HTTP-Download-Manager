import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

public class HttpRangeGetter implements Runnable {
    private String connectionString;
    private BlockingQueue<ChunkData> blockingQueue;
    private int startByte, endByte, chuckSize;

    public HttpRangeGetter(String connectionString, BlockingQueue<ChunkData> blockingQueue, int startByte, int endByte, int chunkSize) {
        this.connectionString = connectionString;
        this.blockingQueue = blockingQueue;
        this.startByte = startByte;
        this.endByte = endByte;
        this.chuckSize = chunkSize;
    }

    @Override
    public void run() {
        BufferedInputStream inputStream = setupConnection();
        if(inputStream == null){
            return;
        }
        var len = 0;
        var currentByte = startByte;
        var readBuffer = new byte[this.chuckSize];
        byte[] chunkBuffer;
        try {
            while ((len = inputStream.read(readBuffer)) != -1) {
                chunkBuffer = new byte[len];
                if (len != readBuffer.length) { // TODO: test how many times this actually happens..
                    System.arraycopy(readBuffer, 0, chunkBuffer, 0, len);
                } else {
                    chunkBuffer = readBuffer;
                }
                var chuckData = new ChunkData(currentByte, len, chunkBuffer);
                System.out.println(String.format("Downloaded %d to %d", currentByte, currentByte+len-1));
//                System.out.println(Arrays.toString(chunkBuffer));
//                blockingQueue.put(chuckData);
                currentByte += len;
            }
        } catch (IOException ioEx) {
            // TODO
//        } catch (InterruptedException e) {
            // TODO
        }
    }

    private BufferedInputStream setupConnection() {
        try {
            var url = new URL(this.connectionString);
            var urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestProperty("Range", String.format("bytes=%d-%d", this.startByte, this.endByte));
            urlConnection.connect();
            return new BufferedInputStream(urlConnection.getInputStream());
        } catch (MalformedURLException malformedEx) {
            // TODO
        } catch (IOException ioEx) {
            // TODO
        }
        return null;
    }
}

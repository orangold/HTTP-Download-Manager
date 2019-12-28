import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

public class IdcDm {
    private static int CHUNK_SIZE = 4096;

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("usage: java IdcDm URL|URL-LIST-FILE [MAX-CONCURRENT-CONNECTIONS]");
            return;
        }
        var firstParam = args[0];
        var isURL = isURL(firstParam);
        ArrayList<String> urlsList;
        String initialURL;

        if (isURL) {
            initialURL = firstParam;
        } else {
            urlsList = getURLsFromFile(firstParam);
            if (urlsList == null) {
                return;
            }
            initialURL = urlsList.get(0);
        }

        var fileSize = getSize(initialURL);
        if (fileSize == -1) {
            return;
        }
        var threadCount = 1;
        if (args.length > 1) {
            threadCount = Integer.parseInt(args[1]);
        }
        var batchSize = fileSize / threadCount;
        var chunkSize = CHUNK_SIZE;
        var currentByte = 0;
        for (int i = 0; i < threadCount; i++) {
            var endByte = (i == threadCount - 1) ? fileSize - 1 : currentByte + batchSize;
            // If dealing with mirrors, set url accordingly.
            Runnable getter = new HttpRangeGetter(firstParam, null, currentByte, endByte, chunkSize);
            getter.run();
            currentByte += batchSize;
        }
    }

    private static ArrayList<String> getURLsFromFile(String fileName) {
        ArrayList<String> urlList = new ArrayList<String>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                urlList.add(line);
            }
        } catch (FileNotFoundException e) {
            System.err.println("URL file list not found!");
            return null;
        } catch (IOException e) {
            System.err.println("Failed to read URLs from file");
            return null;
        }
        if (urlList.size() == 0) {
            System.err.println("No URLs found in file");
            return null;
        }
        return urlList;
    }

    private static boolean isURL(String connString) {
        try {
            new URL(connString);
            return true;
        } catch (MalformedURLException e) {
            return false;
        }
    }

    private static int getSize(String connString) {
        try {
            var url = new URL(connString);
            var urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.connect();
            return urlConnection.getContentLength();
        } catch (MalformedURLException malformedEx) {
            System.err.println("Failed to parse URL, please make sure the URL is in a correct format");
        } catch (IOException ioEx) {
            System.err.println("Failed to fetch file from URL, make sure the URL is correct");
        }
        return -1;
    }
}

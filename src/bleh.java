import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class bleh {
    public static void main(String[] args) {
        meow(args[0]);
    }

    public static byte[] meow(String arg){
        var firstParam = arg;
        var currentURL = firstParam;
        var fileSize = getFileSize(currentURL);
        if (fileSize == -1) {
            return null;
        }
        System.out.println("File size " + fileSize);
        BufferedInputStream inputStream = setupConnection(currentURL);
        var fileName = getFileName(currentURL);
        if (fileName == null) {
            return null;
        }
        var randomAccessFile = getRandomAccessFile("m"+fileName);
        if (randomAccessFile == null) {
            return null;
        }

        int nextInt;
        byte nextByte;
        byte[] arr = new byte[24338431];
        var i=0;
        try {
            while ((nextInt = inputStream.read()) != -1) {
                nextByte = (byte) nextInt;
                arr[i]=nextByte;
                i++;
            }


            System.out.println("Done!");
        } catch (IOException ioEx) {
            // TODO
        }
        try {
            randomAccessFile.seek(0);
            randomAccessFile.write(arr);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return arr;
    }

    private static BufferedInputStream setupConnection(String connectionString) {
        try {
            // NOTE: can request out of range and still be okay ! yay!
            var url = new URL(connectionString);
            var urlConnection = (HttpURLConnection) url.openConnection();
//            var endByte = getEndByte();
            urlConnection.setRequestProperty("Range", String.format("bytes=%d-%d", 0, 24338431));
            System.out.println("Requesting " + 0 + " to " +24338431);
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

    private static RandomAccessFile getRandomAccessFile(String filename) {
        try {
            return new RandomAccessFile(filename, "rws");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            //TODO
        }
        return null;
    }

    private static String getFileName(String connString) {
        try {
            var url = new URL(connString);
            String urlPath = url.getPath();
            return urlPath.substring(urlPath.lastIndexOf('/') + 1);
        } catch (MalformedURLException ex) {
            //todo
        }
        return null;
    }

    private static int getFileSize(String connString) {
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
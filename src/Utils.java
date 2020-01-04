import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Random;

public class Utils {
    public static ArrayList<String> getURLsFromFile(String fileName) {
        var urlList = new ArrayList<String>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                urlList.add(line);
            }
        } catch (FileNotFoundException e) {
            printErrorMessageWithFailure("URL file list not found!");
            return null;
        } catch (IOException e) {
            printErrorMessageWithFailure("Failed to read URLs from file");
            return null;
        }
        if (urlList.size() == 0) {
            printErrorMessageWithFailure("No URLs found in file");
            return null;
        }
        return urlList;
    }

    public static boolean isURL(String connString) {
        try {
            new URL(connString);
            return true;
        } catch (MalformedURLException e) {
            return false;
        }
    }

    public static String getFileName(String connString) {
        try {
            var url = new URL(connString);
            String urlPath = url.getPath();
            return urlPath.substring(urlPath.lastIndexOf('/') + 1);
        } catch (MalformedURLException ex) {
            printErrorMessageWithFailure("Failed to parse URL, please make sure the URL is in a correct format");
        }
        return null;
    }

    public static int getFileSize(String connString) {
        try {
            var url = new URL(connString);
            var urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.connect();
            return urlConnection.getContentLength();
        } catch (MalformedURLException malformedEx) {
            printErrorMessageWithFailure("Failed to parse URL, please make sure the URL is in a correct format");
        } catch (IOException ioEx) {
            printErrorMessageWithFailure("Failed to fetch file, please check your internet connection..");
        }
        return -1;
    }

    public static String getRandomURL(ArrayList<String> URLs) {
        var rand = new Random();
        return URLs.get(rand.nextInt(URLs.size()));
    }

    public static void printErrorMessage(String message) {
        System.err.println(message);
    }

    public static void printErrorMessageWithFailure(String message){
        printErrorMessage(message);
        System.err.println("Download failed");
    }
}

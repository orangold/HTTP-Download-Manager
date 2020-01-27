public class Consts
{
    public static int CHUNK_SIZE = 4096;
    public static int BLOCKING_QUEUE_SIZE = 8192;
    public static String META_DATA_FILE_NAME_SUFFIX = ".chunkmap";
    public static String META_DATA_TEMP_FILE_SUFFIX = "0x0213";
    public static int RANGE_GETTERS_INITITAL_ID = 13; // As mentioned in the Lab example.
    public static int SETUP_CONNECTION_TIMEOUT_MSECONDS = 20000;
    public static int READ_TIMEOUT_MSECONDS = 20000;
    public static int MAX_CONNECTION_RETRIES = 3;
    public static int SAVE_METADATA_PER_CHUNKS_COUNT = 32;
    public static int MIN_FILE_SIZE_FOR_MULTI_THREADING_IN_BYTES = 1000000; // 1MB
    public static int MAX_THREADS_AVAILABLE = 15;
}

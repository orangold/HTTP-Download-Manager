public class RangeGetterChunksData {
    private long startByte;
    private int numOfChunks, startChunkId;

    public RangeGetterChunksData(long startByte, int numOfChunks, int startChunkId) {
        this.startByte = startByte;
        this.numOfChunks = numOfChunks;
        this.startChunkId = startChunkId;
    }

    public long getStartByte() {
        return this.startByte;
    }

    public int getNumOfChunks() {
        return this.numOfChunks;
    }

    public int getStartChunkId() {
        return this.startChunkId;
    }
}

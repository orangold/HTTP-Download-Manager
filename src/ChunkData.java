public class ChunkData {
    private int length, chunkId;
    private long startByte;
    private byte[] data;

    public ChunkData(long startByte, int length, int chunkId, byte[] data) {
        this.startByte = startByte;
        this.length = length;
        this.chunkId = chunkId;
        this.data = data;
    }

    public long getStartByte() {
        return this.startByte;
    }

    public int getLength() {
        return this.length;
    }

    public int getChunkId() {
        return this.chunkId;
    }

    public byte[] getData() {
        return this.data;
    }
}

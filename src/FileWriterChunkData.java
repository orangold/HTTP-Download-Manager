public class FileWriterChunkData {
    private int length, chunkId;
    private long startByte;
    private byte[] data;

    public FileWriterChunkData(long startByte, int length, int chunkId, byte[] data) {
        this.startByte = startByte;
        this.length = length;
        this.chunkId = chunkId;
        this.data = data;
    }

    public FileWriterChunkData(){
        new FileWriterChunkData(0,0,0,null);
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

    public boolean isEmpty() {
        return this.length == 0 && this.chunkId == 0 && this.startByte == 0 && this.data == null;
    }
}

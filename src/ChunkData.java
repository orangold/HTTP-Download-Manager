public class ChunkData {
    private int startByte, length;
    private byte[] data;

    public ChunkData(int startByte, int length, byte[] data) {
        this.startByte = startByte;
        this.length = length;
        this.data = data;
    }

    public int getStartByte() {
        return this.startByte;
    }

    public int getLength() {
        return this.length;
    }

    public byte[] getData() {
        return this.data;
    }
}

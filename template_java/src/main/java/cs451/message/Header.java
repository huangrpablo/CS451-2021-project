package cs451.message;

public class Header {
    private final int id;
    private final MessageType type;
    private final int srcPid;
    private final int destPid;

    public Header(int id, MessageType type, int srcPid, int destPid) {
        this.id = id;
        this.type = type;
        this.srcPid = srcPid;
        this.destPid = destPid;
    }

    public Header(String raw) {
        String[] parts = raw.split(":");
        this.id = Integer.parseInt(parts[0]);
        this.type = MessageType.valueOf(parts[1]);
        this.srcPid = Integer.parseInt(parts[2]);
        this.destPid = Integer.parseInt(parts[3]);
    }

    public MessageType getType() {
        return type;
    }

    public int getSrcPid() {
        return srcPid;
    }

    public int getDestPid() {
        return destPid;
    }

    public int getId() {
        return id;
    }

    public boolean isACK() {
        return MessageType.ACK == type;
    }

    public boolean isData() {
        return MessageType.Data == type;
    }

    public String Serialize() {
        return String.format("%d:%s:%d:%d",
                id, type, srcPid, destPid);
    }

    public String toString() {
        return String.format("Header(id=%d, type=%s, srcPid=%d, destPid=%d)",
                id, type, srcPid, destPid);
    }
}

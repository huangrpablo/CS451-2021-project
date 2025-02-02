package cs451.link.message;

public class Message {
    public Header header;
    public String payload;

    public Message(String raw) {
        int firstCol = raw.indexOf("|");

        try {
            String header = raw.substring(0, firstCol);
            this.header = new Header(header);
            this.payload = raw.substring(firstCol+1);
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(e);
            System.err.println(raw);
        }
    }

    public Message(int id, MessageType type, int srcPid, int destPid, String payload) {
        this.header = new Header(id, type, srcPid, destPid);
        this.payload = payload;
    }

    public Message(int id, MessageType type, int srcPid, int destPid) {
        this.header = new Header(id, type, srcPid, destPid);
        this.payload = "ACK";
    }

    public String Serialize() {
        return String.format("%s|%s", header.Serialize(), payload);
    }

    @Override
    public String toString() {
        return "Message{" +
                "header=" + header +
                ", payload='" + payload + '\'' +
                '}';
    }

    public boolean isACK() {
        return header.isACK();
    }

    public boolean isData() {
        return header.isData();
    }

    public String broadcastLog() {
        return String.format("b %d", header.getId());
    }

    public String deliverLog() {
        return String.format("d %d %d", header.getSrcPid(), header.getId());
    }
}


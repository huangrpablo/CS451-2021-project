package cs451.broadcast;

public class Message implements Comparable<Message> {
    public int oriPid; // origin pid
    public int id;
    public String payload;

    public String serialized;

    public Message(String raw) {
        try {
            int headerCol = raw.indexOf("|");
            String header = raw.substring(0, headerCol);

            String[] ids = header.split(":");
            this.oriPid = Integer.parseInt(ids[0]);
            this.id = Integer.parseInt(ids[1]);
            this.payload = raw.substring(headerCol+1);

            this.serialized = String.format("%d:%d|%s", oriPid, id, payload);
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(e);
            System.err.println(raw);
        }
    }

    public Message(int id, int oriPid, String payload) {
        this.id = id;
        this.oriPid = oriPid;
        this.payload = payload;
        this.serialized = String.format("%d:%d|%s", oriPid, id, payload);
    }

    public String Serialize() {
        return serialized;
    }

    public int hashCode() {
        return serialized.hashCode();
    }

    public boolean equals(Object ob) {
        if (!(ob instanceof Message)) {
            return false;
        }

        Message msg = (Message)ob;
        return this.Serialize().equals(msg.Serialize());
    }

    @Override
    public int compareTo(Message o) {
        int cmp = Integer.compare(this.oriPid, o.oriPid);
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(this.id, o.id);
    }
}

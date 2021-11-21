package cs451.broadcast;

public interface Listener {
    void onDelivery(int oriPid, Message message);
}

package cs451.link;

public interface Listener {
    void onDelivery(int srcPid, String payload);
}

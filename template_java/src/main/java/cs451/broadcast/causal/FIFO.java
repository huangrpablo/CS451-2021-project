package cs451.broadcast.causal;

import cs451.broadcast.Listener;
import cs451.broadcast.Message;
import cs451.broadcast.reliable.UniformRB;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class FIFO {
    private final ConcurrentLinkedQueue<Message> delivered;

    private final UniformRB urb;
    private final Receiver receiver;

    public FIFO(int pid, HashMap<Integer, InetSocketAddress> addresses) {
        Set<Integer> neighbours = addresses.keySet();

        this.delivered = new ConcurrentLinkedQueue<>();
        this.receiver = new Receiver(neighbours, delivered);
        this.urb = new UniformRB(pid, addresses, this.receiver);
    }

    public FIFO(int pid, HashMap<Integer, InetSocketAddress> addresses, Listener listener) {
        this(pid, addresses);
        this.receiver.setListener(listener);
    }

    public void start() {
        this.urb.start();
        new Thread(this.receiver).start();
    }

    public void broadcast(String payload) {
        urb.broadcast(payload);
    }

    public void kill() {
        this.urb.kill();
        this.receiver.kill();
    }

    public String broadcastLog() {
        return this.urb.broadcastLog();
    }

    public String deliverLog() {
        StringBuilder sb = new StringBuilder();

        for (Message data: delivered)
            sb.append(String.format("d %d %d\n", data.oriPid, data.id));

        return sb.toString();
    }

    public void debug() {
        urb.debug();
    }
}

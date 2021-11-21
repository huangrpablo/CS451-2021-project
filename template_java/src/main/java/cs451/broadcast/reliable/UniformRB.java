package cs451.broadcast.reliable;

import cs451.broadcast.Listener;
import cs451.broadcast.Message;
import cs451.link.Link;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

public class UniformRB {
    private final ConcurrentSkipListSet<Message> delivered;

    private final ConcurrentSkipListSet<Message> forward;
    private final ConcurrentLinkedQueue<Message> forForwards;

    private final Broadcaster broadcaster;
    private final Receiver receiver;

    private final int pid;
    private final AtomicInteger count;

    public UniformRB(int pid, HashMap<Integer, InetSocketAddress> addresses) {
        this.pid = pid;
        this.count = new AtomicInteger(1);
        Set<Integer> neighbours = addresses.keySet();

        this.delivered = new ConcurrentSkipListSet<>();
        this.forward = new ConcurrentSkipListSet<>();
        this.forForwards = new ConcurrentLinkedQueue<>();

        this.receiver = new Receiver(neighbours, delivered, forward, forForwards);
        this.broadcaster = new Broadcaster(new Link(pid, addresses, this.receiver), neighbours, forForwards);
    }

    public UniformRB(int pid, HashMap<Integer, InetSocketAddress> addresses, Listener listener) {
        this(pid, addresses);
        this.receiver.setListener(listener);
    }

    public void start() {
        new Thread(this.receiver).start();
        new Thread(this.broadcaster).start();
        System.out.println("URB starts...");
    }


    public void broadcast(String payload) {
        int id = count.getAndIncrement();
        Message data = new Message(id, pid, payload);

        forward.add(data);
        forForwards.add(data);
    }

    public void kill() {
        this.broadcaster.kill();
        this.receiver.kill();
    }

    public String broadcastLog() {
        StringBuilder sb = new StringBuilder();

        int current = count.get();
        for (int i = 1; i < current; i++)
            sb.append(String.format("b %d\n", i));

        return sb.toString();
    }

    public String deliverLog() {
        StringBuilder sb = new StringBuilder();

        for (Message data: delivered)
            sb.append(String.format("d %d %d\n", data.oriPid, data.id));

        return sb.toString();
    }

    public void debug() {
        this.receiver.debug();
    }
}

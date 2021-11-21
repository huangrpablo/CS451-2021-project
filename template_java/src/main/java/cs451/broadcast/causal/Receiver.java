package cs451.broadcast.causal;

import cs451.broadcast.Listener;
import cs451.broadcast.Message;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;

public class Receiver implements Runnable, Listener {
    private final Set<Integer> neighbours;
    private final ConcurrentLinkedQueue<Message> delivered;
    private final ConcurrentHashMap<Integer, PriorityBlockingQueue<Message>> pending;
    private final HashMap<Integer, Integer> next;

    private Listener listener;

    private boolean stopped = false;

    public Receiver(Set<Integer> neighbours,
                    ConcurrentLinkedQueue<Message> delivered) {
        this.neighbours = neighbours;
        this.delivered = delivered;
        this.next = new HashMap<>();
        this.pending = new ConcurrentHashMap<>();


        for (Integer neighbour : neighbours) {
            next.put(neighbour, 1);
            pending.put(neighbour, new PriorityBlockingQueue<>());
        }
    }

    public Receiver(Set<Integer> neighbours,
                    ConcurrentLinkedQueue<Message> delivered,
                    Listener listener) {
        this(neighbours, delivered);
        this.listener = listener;
    }

    @Override
    public void run() {
        while (!stopped) {
            for (int neighbour : neighbours) {
                tryDeliver(neighbour);
            }
        }
    }

    @Override
    public void onDelivery(int oriPid, Message message) {
        System.out.printf("FIFO.onDelivery: oriPid=%d, payload=%s\n", oriPid, message.payload);
        pending.get(oriPid).offer(message);
    }

    public void kill() {
        this.stopped = true;
    }

    private void tryDeliver(int neighbour) {
        int next = this.next.get(neighbour);
        PriorityBlockingQueue<Message> pending = this.pending.get(neighbour);

        if (stopped || pending.isEmpty()) {
            return;
        }

        Message data = pending.peek();
        while (!stopped && data != null && data.id == next) {
            delivered.offer(data);
            pending.poll();  next++;

            if (listener != null)
                listener.onDelivery(data.oriPid, data);

            System.out.printf("FIFO.deliver: from=%d, id=%d\n", data.oriPid, data.id);

            data = pending.peek();
        }

        this.next.put(neighbour, next);
    }

    public void setListener(Listener listener) {
        this.listener = listener;
    }
}

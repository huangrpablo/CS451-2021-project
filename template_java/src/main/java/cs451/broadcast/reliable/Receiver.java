package cs451.broadcast.reliable;

import cs451.broadcast.Listener;
import cs451.broadcast.Message;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

public class Receiver implements Runnable, cs451.link.Listener {
    private final Set<Integer> neighbours;
    private final ConcurrentSkipListSet<Message> delivered;
    private final ConcurrentSkipListSet<Message> forward;
    private final ConcurrentLinkedQueue<Message> forForwards;
    private final ConcurrentHashMap<Message, ConcurrentSkipListSet<Integer>> ack;

    private Listener listener;

    private boolean stopped = false;

    public Receiver(Set<Integer> neighbours,
                    ConcurrentSkipListSet<Message> delivered,
                    ConcurrentSkipListSet<Message> forward,
                    ConcurrentLinkedQueue<Message> forForwards) {
        this.neighbours = neighbours;
        this.delivered = delivered;
        this.forward = forward;
        this.forForwards = forForwards;
        this.ack = new ConcurrentHashMap<>();
    }

    @Override
    public void onDelivery(int srcPid, String payload) {
        System.out.printf("URB.onDelivery: srcPid=%d, payload=%s\n", srcPid, payload);
        Message data = new Message(payload);

        if (!ack.containsKey(data)) {
            ack.put(data, new ConcurrentSkipListSet<>());
        }

        ack.get(data).add(srcPid);

        if (!forward.contains(data)) {
            forward.add(data);
            forForwards.add(data);
        }
    }

    @Override
    public void run() {
        while (!stopped) {
            for (Message data : forward) {
                tryDeliver(data);
            }
        }
    }

    private void tryDeliver(Message data) {
        if (stopped || !isDeliverable(data)) {
            return;
        }

        delivered.add(data);

        if (listener == null) {
            return;
        }

        listener.onDelivery(data.oriPid, data);
    }

    private boolean isDeliverable(Message data) {
        if (delivered.contains(data)) {
            return false;
        }

        if (!ack.containsKey(data)) {
            return false;
        }

        return (ack.get(data).size()+1) > neighbours.size() / 2;
    }


    public void kill() {
        this.stopped = true;
    }

    public void setListener(Listener listener) {
        this.listener = listener;
    }

    public void debug() {
        for (Message data : ack.keySet()) {
            System.out.println(data.Serialize());
            for (Integer i : ack.get(data))
                System.out.printf("%d ", i);

            System.out.println();
        }
    }

}

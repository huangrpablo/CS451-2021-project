package cs451.broadcast.reliable;

import cs451.broadcast.Message;
import cs451.link.Link;

import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Broadcaster implements Runnable {
    private final Link link;
    private final ConcurrentLinkedQueue<Message> forForwards;

    private final Set<Integer> neighbours;

    private boolean stopped = false;

    public Broadcaster(Link link,
                       Set<Integer> neighbours,
                       ConcurrentLinkedQueue<Message> forForwards) {
        this.link = link;
        this.neighbours = neighbours;
        this.forForwards = forForwards;

        this.link.start();
        System.out.println("Link starts...");

    }

    @Override
    public void run() {
        while (!stopped) {
            while (!stopped && !forForwards.isEmpty()) {
                bestEffort(forForwards.poll());
            }
        }
    }

    private void bestEffort(Message payload) {
        for (Integer neighbour : neighbours) {
            link.send(payload.Serialize(), neighbour);
        }
    }

    public void kill() {
        this.stopped = true;
        this.link.kill();
    }

}

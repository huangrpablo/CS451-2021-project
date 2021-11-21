package cs451.link;

import cs451.link.message.Message;
import cs451.link.message.MessageType;

import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

public class Link {
    private final ConcurrentLinkedQueue<Message> forSends;
    private final ConcurrentHashMap<String, Message> delivered;

    private DatagramSocket socket;

    private final Sender sender;
    private final Receiver receiver;

    private final int pid;
    private final AtomicInteger count;

    public Link(int pid, HashMap<Integer, InetSocketAddress> addresses) {
        this.pid = pid;
        this.count = new AtomicInteger();
        this.forSends = new ConcurrentLinkedQueue<>();
        this.delivered = new ConcurrentHashMap<>();
        ConcurrentSkipListSet<String> ack = new ConcurrentSkipListSet<>();
        ConcurrentLinkedQueue<Message> forACKs = new ConcurrentLinkedQueue<>();

        try {
            int port = addresses.get(pid).getPort();
            System.out.printf("%d tries to bind on port %d\n", pid, port);
            this.socket = new DatagramSocket(port);
        } catch (SocketException e) {
            System.err.println(e);
        }

        this.sender = new Sender(forSends, forACKs, ack, pid, socket, addresses);
        this.receiver = new Receiver(delivered, forACKs, ack, pid, socket);
    }

    public Link(int pid, HashMap<Integer, InetSocketAddress> addresses, Listener listener) {
        this(pid, addresses);
        this.receiver.setListener(listener);
    }

    public int getPid() {
        return pid;
    }

    public void start() {
        new Thread(this.sender).start();
        new Thread(this.receiver).start();
    }

    public void send(String payload, int destPid) {
        if (destPid == pid) {
            return;
        }

        int id = count.getAndIncrement();
        Message data = new Message(id, MessageType.Data, pid, destPid, payload);
        forSends.add(data);
    }

    public void kill() {
        this.sender.kill();
        this.receiver.kill();
    }

    public String broadcastLog() {
        StringBuilder sb = new StringBuilder();

        int current = count.get();
        for (int i = 0; i < current; i++)
            sb.append(String.format("b %d\n", i));

        return sb.toString();
    }

    public String deliverLog() {
        StringBuilder sb = new StringBuilder();

        for (Message data : delivered.values())
            sb.append(String.format("d %d %d\n", data.header.getSrcPid(), data.header.getId()));

        return sb.toString();
    }
}

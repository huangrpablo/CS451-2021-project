package cs451.link;

import cs451.message.Message;
import cs451.message.MessageType;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

public class Link {
    private final List<Message> forSends;
    private final ConcurrentHashMap<String, Message> delivered;
    private ConcurrentSkipListSet<String> ack;
    private ConcurrentLinkedQueue<Message> forACKs;

    private HashMap<Integer, InetSocketAddress> addresses;
    private DatagramSocket socket;

    private final Sender sender;
    private final Receiver receiver;

    private final int pid;
    private final AtomicInteger count;

    private String output;

    public Link(int pid, HashMap<Integer, InetSocketAddress> addresses) {
        this.pid = pid;
        this.count = new AtomicInteger();
        this.forSends = new ArrayList<>();
        this.delivered = new ConcurrentHashMap<>();
        this.ack = new ConcurrentSkipListSet<>();
        this.forACKs = new ConcurrentLinkedQueue<>();

        this.addresses = addresses;

        try {
            int port = this.addresses.get(pid).getPort();
            this.socket = new DatagramSocket(port);
        } catch (SocketException e) {
            System.err.println(e);
        }

        this.sender = new Sender(forSends, forACKs, ack, pid, socket, addresses);
        this.receiver = new Receiver(delivered, forACKs, ack, pid, socket);
    }

    public Link(int pid, HashMap<Integer, InetSocketAddress> addresses, String output) {
        this(pid, addresses);
        this.output = output;
    }

    public int getPid() {
        return pid;
    }

    public void start() {
        new Thread(this.sender).start();
        new Thread(this.receiver).start();

        System.out.printf("%d: Sender and receiver are started...\n", pid);
    }

    public void send(String payload, int destPid) {
        int id = count.getAndIncrement();
        Message data = new Message(id, MessageType.Data, pid, destPid, payload);
        forSends.add(data);
    }

    public void kill() {
        this.sender.kill();
        this.receiver.kill();
    }

    public void flush2File() {
        try {
            FileWriter fw = new FileWriter(output);
            BufferedWriter bw = new BufferedWriter(fw);

            for (Message data: forSends) {
                bw.write(data.broadcastLog() + "\n");
            }

            for (Message data: delivered.values()) {
                bw.write(data.deliverLog() + "\n");
            }

            bw.close();
        } catch (IOException e) {
            System.err.println(e);
        }
    }
}

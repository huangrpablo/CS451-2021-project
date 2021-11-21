package cs451.link;

import cs451.link.message.Header;
import cs451.link.message.Message;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

public class Sender implements Runnable {
    private final ConcurrentLinkedQueue<Message> forSends;
    private final ConcurrentLinkedQueue<Message> forACKs;
    private final ConcurrentSkipListSet<String> ack;
    private final HashMap<Integer, InetSocketAddress> addresses;
    private final int localPid;

    private final DatagramSocket socket;

    private boolean stopped = false;

    public Sender(ConcurrentLinkedQueue<Message> forSends,
                  ConcurrentLinkedQueue<Message> forACKs,
                  ConcurrentSkipListSet<String> ack,
                  int localPid, DatagramSocket socket,
                  HashMap<Integer, InetSocketAddress> addresses) {
        this.ack = ack;
        this.forSends = forSends;
        this.forACKs = forACKs;
        this.addresses = addresses;
        this.localPid = localPid;
        this.socket = socket;
    }

    @Override
    public void run() {
        while (!stopped) {
            for (Message forSend : forSends) {
                if (!stopped && !isACKed(forSend))
                    send(forSend);
            }

            while (!stopped && !forACKs.isEmpty()) {
                send(forACKs.poll());
            }
        }

        System.out.printf("%d: Link Sender is killed!", localPid);
    }

    private void send(Message data) {
        try {
            int remotePid = data.header.getDestPid();
            InetSocketAddress remote = addresses.get(remotePid);

            byte[] buffer = data.Serialize().getBytes(StandardCharsets.UTF_8);
            DatagramPacket packet = new DatagramPacket(
                    buffer, buffer.length, remote
            );

            socket.send(packet);
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    private boolean isACKed(Message data) {
        Header header = data.header;

        String key = String.format(
                "%d:%d",
                header.getDestPid(),
                header.getId()
        );

        return ack.contains(key);
    }

    public void kill() {
        this.stopped = true;
    }
}

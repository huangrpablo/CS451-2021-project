package cs451.link;

import cs451.message.Header;
import cs451.message.Message;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

public class Sender implements Runnable {
    private final List<Message> forSends;
    private final ConcurrentSkipListSet<String> ack;
    private final HashMap<Integer, InetSocketAddress> addresses;
    private final int localPid;

    private final DatagramSocket socket;

    private boolean stopped = false;

    public Sender(List<Message> forSends,
                  ConcurrentSkipListSet<String> ack,
                  int localPid, DatagramSocket socket,
                  HashMap<Integer, InetSocketAddress> addresses) {
        this.ack = ack;
        this.forSends = forSends;
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

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                System.err.println(e);
            }
        }

        System.out.printf("%d:Sender is killed!", localPid);
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

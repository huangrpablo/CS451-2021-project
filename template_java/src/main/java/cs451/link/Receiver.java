package cs451.link;

import cs451.message.Header;
import cs451.message.Message;
import cs451.message.MessageType;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class Receiver implements Runnable {
    private final ConcurrentHashMap<String, Message> delivered;
    private final ConcurrentSkipListSet<String> ack;
    private final HashMap<Integer, InetSocketAddress> addresses;

    private final int localPid;

    private final DatagramPacket packet;
    private final DatagramSocket socket;

    private boolean stopped = false;

    public Receiver(ConcurrentHashMap<String, Message> delivered,
                    ConcurrentSkipListSet<String> ack,
                    int localPid, DatagramSocket socket,
                    HashMap<Integer, InetSocketAddress> addresses) {
        this.ack = ack;
        this.delivered = delivered;
        this.addresses = addresses;
        this.localPid = localPid;
        this.socket = socket;

        byte[] buffer = new byte[65536];
        this.packet = new DatagramPacket(buffer, buffer.length);
    }

    @Override
    public void run() {
        while(!stopped) {
            try {
                socket.receive(packet);

                String raw = new String(packet.getData(),
                        0, packet.getLength(),
                        StandardCharsets.UTF_8);

                Message message = new Message(raw);

                if (message.isData()) {
                    processData(message);
                }

                if (message.isACK()) {
                    processACK(message);
                }

            } catch (IOException e) {
                System.err.println(e);
            }
        }

        System.out.printf("%d:Receiver is killed!\n", localPid);
    }

    private void processData(Message data) {
        Header header = data.header;

        String key = String.format(
                "%d:%d",
                header.getSrcPid(),
                header.getId()
        );

        delivered.putIfAbsent(key, data);
        System.out.printf("%d:Recv Data=%s\n", localPid, data);
        ackIt(data);
    }

    private void ackIt(Message data) {
        try {
            Header header = data.header;
            Message ack = new Message(
                    header.getId(),
                    MessageType.ACK,
                    localPid,
                    header.getSrcPid()
            );

            int remotePid = data.header.getSrcPid();
            InetSocketAddress remote = addresses.get(remotePid);

            byte[] buffer = ack.Serialize().getBytes(StandardCharsets.UTF_8);
            packet.setSocketAddress(remote);
            packet.setData(buffer, 0, buffer.length);

            socket.send(packet);

            System.out.printf("%d:Send ACK=%s\n", localPid, ack);
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    private void processACK(Message ack) {
        Header header = ack.header;

        String key = String.format(
                "%d:%d",
                header.getSrcPid(),
                header.getId()
        );

        this.ack.add(key);
        System.out.printf("%d:Recv ACK=%s\n", localPid, ack);
    }

    public void kill() {
        this.stopped = true;
    }
}

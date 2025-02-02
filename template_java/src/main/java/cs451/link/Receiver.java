package cs451.link;

import cs451.link.message.Header;
import cs451.link.message.Message;
import cs451.link.message.MessageType;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

public class Receiver implements Runnable {
    private final ConcurrentHashMap<String, Message> delivered;
    private final ConcurrentLinkedQueue<Message> forACKs;
    private final ConcurrentSkipListSet<String> ack;

    private Listener listener;

    private final int localPid;

    private final DatagramSocket socket;

    private final byte[] buffer = new byte[65536];

    private boolean stopped = false;

    public Receiver(ConcurrentHashMap<String, Message> delivered,
                    ConcurrentLinkedQueue<Message> forACKs,
                    ConcurrentSkipListSet<String> ack,
                    int localPid, DatagramSocket socket) {
        this.ack = ack;
        this.delivered = delivered;
        this.forACKs = forACKs;
        this.localPid = localPid;
        this.socket = socket;

        System.out.printf("Start at %d\n", System.currentTimeMillis());
    }

    @Override
    public void run() {
        while(!stopped) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

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



        if (!delivered.containsKey(key)) {
            delivered.put(key, data);

            onDelivery(data);
        }

        ackIt(data);
    }

    private void onDelivery(Message data) {
        if (listener == null) {
            return;
        }

        int srcPid = data.header.getSrcPid();
        String payload = data.payload;

        listener.onDelivery(srcPid, payload);
    }

    private void ackIt(Message data) {
        Header header = data.header;
        Message ack = new Message(
                header.getId(),
                MessageType.ACK,
                localPid,
                header.getSrcPid()
        );

        forACKs.offer(ack);
    }

    private void processACK(Message ack) {
        Header header = ack.header;

        String key = String.format(
                "%d:%d",
                header.getSrcPid(),
                header.getId()
        );

        this.ack.add(key);
    }

    public void kill() {
        this.stopped = true;
    }

    public void setListener(Listener listener) {
        this.listener = listener;
    }
}

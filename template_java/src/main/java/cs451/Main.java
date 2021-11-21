package cs451;

import cs451.broadcast.causal.FIFO;
import cs451.link.Link;
import cs451.link.message.Message;
import cs451.utils.Generator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class Main {
    private static FIFO fifo;
    private static String output;

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        fifo.kill();

        //write/flush output file if necessary
        System.out.println("Writing output.");

        flush2File();

        System.out.flush();
    }

    private static void flush2File() {
        try {
            FileWriter fw = new FileWriter(output);
            BufferedWriter bw = new BufferedWriter(fw);

            bw.write(fifo.broadcastLog());
            bw.write(fifo.deliverLog());

            bw.close();
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    private static FIFO initFIFO(Parser parser) {
        HashMap<Integer, InetSocketAddress> addresses = new HashMap<>();
        for (Host host: parser.hosts()) {
            InetSocketAddress address = new InetSocketAddress(
                    host.getIp(), host.getPort());
            addresses.put(host.getId(), address);
        }

        FIFO fifo = new FIFO(parser.myId(), addresses);

        output = parser.output();

        int numOfMessage = 0;

        try {
            String config = Files.readString(Path.of(parser.config()));
            numOfMessage = Integer.parseInt(config.strip());
        } catch (IOException e) {
            System.err.println(e);
            System.exit(-1);
        }

        String[] messages = Generator.range(0, numOfMessage);
        for (String message: messages) {
            fifo.broadcast(message);
        }

        return fifo;
    }

    public static void main(String[] args) throws InterruptedException {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID: " + pid + "\n");
        System.out.println("From a new terminal type `kill -SIGINT " + pid + "` or `kill -SIGTERM " + pid + "` to stop processing packets\n");

        System.out.println("My ID: " + parser.myId() + "\n");
        System.out.println("List of resolved hosts is:");
        System.out.println("==========================");
        for (Host host: parser.hosts()) {
            System.out.println(host.getId());
            System.out.println("Human-readable IP: " + host.getIp());
            System.out.println("Human-readable Port: " + host.getPort());
            System.out.println();
        }
        System.out.println();

        System.out.println("Path to output:");
        System.out.println("===============");
        System.out.println(parser.output() + "\n");

        System.out.println("Path to config:");
        System.out.println("===============");
        System.out.println(parser.config() + "\n");

        System.out.println("Doing some initialization\n");

        fifo = initFIFO(parser);
        fifo.start();

        System.out.println("FIFO starts...\n");

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }
}

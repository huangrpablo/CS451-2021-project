package cs451;

import cs451.link.Link;
import cs451.utils.Generator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class Main {
    private static Link link;

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        link.kill();

        //write/flush output file if necessary
        System.out.println("Writing output.");

        link.flush2File();

        System.out.flush();
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    private static Link initLink(Parser parser) {
        int id = parser.myId();
        HashMap<Integer, InetSocketAddress> addresses = new HashMap<>();
        for (Host host: parser.hosts()) {
            InetSocketAddress address = new InetSocketAddress(
                    host.getIp(), host.getPort());
            addresses.put(host.getId(), address);
        }

        Link link = new Link(id, addresses, parser.output());

        System.out.printf("%d:Link is created successfully...\n", id);

        // send messages

        int numOfMessage = 0;
        int target = 0;

        try {
            String config = Files.readString(Path.of(parser.config()));
            String[] configs = config.split(" ");
            numOfMessage = Integer.parseInt(configs[0].strip());
            target = Integer.parseInt(configs[1].strip());
        } catch (IOException e) {
            System.err.println(e);
            System.exit(-1);
        }

        if (link.getPid() == target) {
            return link;
        }

        System.out.printf("%d:Link is a sender...\n", id);

        String[] messages = Generator.range(0, numOfMessage);
        for (String message: messages) {
            link.send(message, target);
        }

        System.out.printf("%d:Link adds %d# messages to queue...\n", id, numOfMessage);

        return link;
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

        link = initLink(parser);
        link.start();

        System.out.println("Broadcasting and delivering messages...\n");

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }
}

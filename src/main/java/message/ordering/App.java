/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package message.ordering;

import org.apache.pulsar.client.api.*;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class App {

    private static final String TOPIC_NAME = "wav-"+ System.currentTimeMillis();
    private static final String INPUT_FILE_NAME = "src/main/resources/CantinaBand60.wav";
    private static final String OUTPUT_FILE_NAME = "/tmp/"+TOPIC_NAME+".wav";
    public static final String END_OF_STREAM_MARKER = "EOS";
    private static final int BUFFER_READ_SIZE = 1024;
    private PulsarClient client;
    private boolean asynchronous = false;

    public App() throws IOException, InterruptedException {
        client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        Runnable consumer = () -> {
            try {
                Consumer consumer1 = client.newConsumer()
                        .topic(TOPIC_NAME)
                        .subscriptionName("my-subscription")
                        .subscribe();

                boolean eosSeen = false;
                // Wait for a message
                int total = 0;
                File outputFile = new File(OUTPUT_FILE_NAME);
                FileOutputStream fos = new FileOutputStream(outputFile);
                while (eosSeen == false) {
                    Message msg = consumer1.receive();
                    fos.write(msg.getData());
                    total += msg.getData().length;
                    consumer1.acknowledge(msg);
                    if ("true".equals(msg.getProperty(END_OF_STREAM_MARKER))) {
                        eosSeen = true;
                    }
                }
                System.out.println("Received " + total + " bytes");
                fos.close();
                System.out.println("Output file:  " + OUTPUT_FILE_NAME);
                consumer1.close();
            } catch(Exception e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
            }
        };

        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        Runnable producer = () -> {
            try {
                List<CompletableFuture> futureList = new ArrayList();
                Producer<byte[]> producer1 = client.newProducer()
                        //.batchingMaxMessages(1)
                        .blockIfQueueFull(true)
                        .topic(TOPIC_NAME)
                        .create();

                File inputFile = new File(INPUT_FILE_NAME);
                InputStream inputStream = new DataInputStream(new FileInputStream(inputFile));
                byte[] buffer = new byte[BUFFER_READ_SIZE];
                int len;
                int total = 0;
                while ((len = inputStream.read(buffer)) != -1) {
                    if(len == BUFFER_READ_SIZE) {
                        if(asynchronous) {
                            futureList.add(producer1.sendAsync(buffer));
                        } else {
                            producer1.send(buffer);
                        }
                    } else {
                        if(asynchronous) {
                            futureList.add(producer1.sendAsync(Arrays.copyOfRange(buffer, 0, len)));
                        } else {
                            producer1.sendAsync(Arrays.copyOfRange(buffer, 0, len));
                        }
                    }
                    total += len;
                }
                System.out.println("Sent a total of " + total + " bytes");
                TypedMessageBuilder message = producer1.newMessage();
                message.property(END_OF_STREAM_MARKER, "true");
                futureList.add(message.sendAsync());
                for(CompletableFuture f : futureList ) {
                    f.join();
                }
                producer1.close();
            } catch(Exception e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
            }
        };
        Thread producerThread = new Thread(producer);
        producerThread.start();

        producerThread.join();
        consumerThread.join();


        client.close();

        System.out.println("File Sizes same? " + fileSizesAreTheSame(INPUT_FILE_NAME, OUTPUT_FILE_NAME));
        System.out.println("File contents same? " + fileContentsAreTheSame(INPUT_FILE_NAME, OUTPUT_FILE_NAME));
    }

    private boolean fileSizesAreTheSame(String s1, String s2) {
        File f1 = new File(s1);
        File f2 = new File(s2);
        long difference = Math.abs(f1.length() - f2.length());
        return difference == 0;
    }

    private boolean fileContentsAreTheSame(String s1, String s2) {
        File f1 = new File(s1);
        File f2 = new File(s2);
        try {
            byte[] file1Contents = Files.readAllBytes(f1.toPath());
            byte[] file2Contents = Files.readAllBytes(f2.toPath());
            int diff = 0;
            for (int i = 0; i < Math.min(file1Contents.length, file2Contents.length); i++) {
                if (file1Contents[i] != file2Contents[i]) {
                    diff++;
                }
            }
            if (diff != 0) {
                System.err.println("Byte difference " + diff + " out of " + file1Contents.length + " total bytes");
            }
            return diff == 0;
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        App a = new App();
    }
}

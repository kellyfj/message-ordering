/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package message.ordering;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.shade.org.apache.commons.codec.digest.DigestUtils;
import org.apache.pulsar.shade.org.apache.commons.lang.RandomStringUtils;

import java.io.*;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class App {

    private static final String TOPIC_NAME = "wav-"+ System.currentTimeMillis();
    private static final String INPUT_FILE_NAME = "src/main/resources/CantinaBand60.wav";
    private static final String OUTPUT_FILE_NAME = "/tmp/"+TOPIC_NAME+".wav";
    public static final String END_OF_STREAM_MARKER = "EOS";
    public static final String MSG_NUMBER = "msgNum";
    public static final String MSG_SHA = "msgSha";
    private static final int BUFFER_READ_SIZE = 1024;
    private PulsarClient client;

    public App(boolean asynchronous, int batchingMaxMessages) throws IOException, InterruptedException {
        client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        Runnable consumer = () -> {
            try {
                Consumer consumer1 = client.newConsumer()
                        .topic(TOPIC_NAME)
                        .subscriptionName("my-subscription-" + RandomStringUtils.randomAlphanumeric(5))
                        .subscriptionType(SubscriptionType.Exclusive)
                        .subscribe();

                boolean eosSeen = false;
                // Wait for a message
                int total = 0;
                File outputFile = new File(OUTPUT_FILE_NAME);
                FileOutputStream fos = new FileOutputStream(outputFile);
                while (eosSeen == false) {
                    Message msg = consumer1.receive();
                    byte[] data = msg.getData();
                    fos.write(data);
                    total += msg.getData().length;
                    consumer1.acknowledge(msg);
                    String msgNum = msg.getProperty(MSG_NUMBER);
                    String sha = msg.getProperty(MSG_SHA);
                    MessageDigest md = MessageDigest.getInstance("SHA-1");
                    md.update(data);
                    String newSha = DigestUtils.md2Hex(md.digest());
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
                        .batchingMaxMessages(batchingMaxMessages)
                        .batchingMaxPublishDelay(100, TimeUnit.SECONDS)
                        .blockIfQueueFull(true)
                        .topic(TOPIC_NAME)
                        .create();

                File inputFile = new File(INPUT_FILE_NAME);
                InputStream inputStream = new DataInputStream(new FileInputStream(inputFile));
                byte[] buffer = new byte[BUFFER_READ_SIZE];
                int len;
                int total = 0;
                int count = 0;
                TypedMessageBuilder message;
                while ((len = inputStream.read(buffer)) != -1) {
                    message = producer1.newMessage();
                    byte[] sendBuf;
                    sendBuf = Arrays.copyOfRange(buffer, 0, len);
                    message.value(sendBuf);
                    message.property(MSG_NUMBER, String.valueOf(count));
                    MessageDigest md = MessageDigest.getInstance("SHA-1");
                    md.update(sendBuf);
                    message.property(MSG_SHA, DigestUtils.md2Hex(md.digest()));
                    if(asynchronous) {
                        futureList.add(message.sendAsync());
                    } else {
                        message.send();
                    }
                    total += len;
                    count += 1;
                }
                System.out.println("Sent a total of " + total + " bytes");
                message = producer1.newMessage();
                message.property(END_OF_STREAM_MARKER, "true");
                if(asynchronous) {
                    futureList.add(message.sendAsync());
                } else {
                    message.send();
                }
                producer1.flush();
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()])).join();
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
        if(args.length != 2) {
            System.err.println("Arguments (sync|async) (maxBatchSize)");
            System.exit(-1);
        }

        String syncOrAsync = args[0];
        boolean async = false;
        if("sync".equals(syncOrAsync) || "async".equals(syncOrAsync)) {
            async = "async".equals(syncOrAsync);
        } else {
            System.err.println("Arguments (sync|async) (maxBatchSize)");
            System.exit(-1);
        }
        int maxBatchingSize = Integer.parseInt(args[1]);

        App a = new App(async, maxBatchingSize);
    }
}

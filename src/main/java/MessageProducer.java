import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Junfeng on 2015/4/8.
 */
public class MessageProducer extends Thread {
    private static Logger logger = Logger.getLogger(MessageProducer.class);
    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final String topic="spaceStatus";
    private final String directoryPath;
    private final Properties props = new Properties();
    Queue<String> inQueue ;
    TCPReceiver tcpReceiver;

    /**
     * Instantiates a new kafka producer.
     *
     *
     * @param directoryPath the directory path
     */
    public MessageProducer( String directoryPath) {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "localhost:9092");
        producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
        //this.topic = topic;
        this.directoryPath = directoryPath;
        inQueue = new ConcurrentLinkedQueue<String>();
        tcpReceiver = new TCPReceiver(inQueue);
    }


    @Override
    public void run() {
        //logger.debug("Producer is running...");
        System.out.println("Producer is running...");
        tcpReceiver.start();
        while (true){
            if(!inQueue.isEmpty()){
                String messageStr = inQueue.poll();
                KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>(topic, messageStr);
                producer.send(data);
                System.out.println(messageStr+" is sent");
            }
        }
        //producer.close();
    }

    public static void main(String[] args){
        MessageProducer messageProducer = new MessageProducer("");
        messageProducer.start();
    }

}

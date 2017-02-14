

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;


/**
 * Created byhong on 2017/2/10.
 */
public class KafkaProducer extends  Thread{
    private final Producer<Integer, String> producer;
    private  String topic="topic1";
    private final Properties props = new Properties();
    public KafkaProducer(String topic)
    {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "192.168.31.131:9091,192.168.31.131:9092,192.168.31.131:9093");
        props.put("zookeeper.connect", "l27.0.0.1:2181");
        // Use random partitioner. Don't need the key type. Just set it to Integer.
        // The message is of type String.
        producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
    }

    public void run() {
        int messageNo = 1;
        while(true)
        {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

                 String  messageStr= new String("Message_" + messageNo);
                 System.out.print(messageStr);
                 try {
                     producer.send(new KeyedMessage<Integer, String>("topic", messageStr));
                 }catch (Exception e){
                    e.printStackTrace();
                 }

                messageNo++;

        }
    }

    public static void main(String[] args) {
        KafkaProducer producerThread = new KafkaProducer("topic");
        producerThread.start();
    }
}

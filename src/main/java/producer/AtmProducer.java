/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package producer;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 * @author fernando
 */
public class AtmProducer {

    public static final int _MULTIPLES_OF = 10;
    public static final int _NUM_OF_OPERATIONS = 4;
    public static final int _MIN_SLEEP = 3000;
    public static final int _MAX_SLEEP = 15000;
    public static final int _NUM_ATMS = 10;

    public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"atmRecord\","
            + "\"fields\":["
            + "  { \"name\":\"id\", \"type\":\"string\" },"
            + "  { \"name\":\"operValue\", \"type\":\"int\" }"
            + "]}";

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 1; i < _NUM_ATMS; i++) {
            executorService.execute(new AtmProducer.ProducerThread(Integer.toString(i)));
        }
        executorService.shutdown();

    }

    static class ProducerThread implements Runnable {

        private String id;

        ProducerThread(String id) {
            this.id = id;
        }

        public void run() {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092,localhost:9093");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(USER_SCHEMA);
            Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
            KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

            for (int i = 0; i < _NUM_OF_OPERATIONS; i++) {

                try {
                    Double sleepTime = _MIN_SLEEP + Math.random() * (_MAX_SLEEP - _MIN_SLEEP);
                    Thread.sleep(sleepTime.intValue());
                } catch (InterruptedException ex) {
                    Logger.getLogger(AtmProducer.class.getName()).log(Level.SEVERE, null, ex);
                }
                
                GenericData.Record avroRecord = new GenericData.Record(schema);
                avroRecord.put("id", id);
                avroRecord.put("operValue", _MULTIPLES_OF * (1 + (int) Math.floor(Math.random() * 50)));

                byte[] bytes = recordInjection.apply(avroRecord);

                //ProducerRecord<String, byte[]> record = new ProducerRecord<>("atmOperations", bytes);
                ProducerRecord<String, byte[]> record = new ProducerRecord<>("atmOperations", id, bytes);
                producer.send(record);                
                System.out.println(recordInjection.invert(bytes).get().toString());

            }

            producer.close();

        }

    }

}

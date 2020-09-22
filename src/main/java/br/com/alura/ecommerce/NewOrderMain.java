package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties());
        KafkaProducer<String, String> producerRetry = new KafkaProducer<String, String>(properties());

        String value = "pedido, usuário, valorDaCompra";
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("TOPICO_NORMAL", value, value);
        ProducerRecord<String, String> recordRETRY = new ProducerRecord<String, String>("TOPICO_NORMAL_RETRY", value, value);


//        Callback callback = callBack;
//        producer.send(record, callback);

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println(recordMetadata);
                System.out.println(e);
            }
        });

        producerRetry.send(record, (data, ex) -> { // chamada de retorno
            if (ex == null) {
                producer.send(recordRETRY, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        System.out.println(recordMetadata);
                        System.out.println(e);
                    }
                });
                return;
            }

            System.out.println("sucesso enviando nesse topico " + data.topic() + ":::partition" + data.partition() + "/ offset " + data.offset() + "/ timestamp: " + data.timestamp());
        }).get();
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}

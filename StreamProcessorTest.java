package com.tsb.ods.processor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import com.datastax.oss.driver.api.core.CqlSession;
import com.tsb.ods.stream.schema.avro.BS95.BS95Product;

import lombok.SneakyThrows;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
		partitions = 1,
		controlledShutdown = true,
		bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
public class StreamProcessorTest {

	@Value("${topic.dlqname}")
	private String dlqTopic;
    
    private static final long TIMEOUT = 5L;
    
    private CountDownLatch eventCounsumedLatch = new CountDownLatch(1);
    
    @Autowired
    KafkaTemplate<String, BS95Product> kafkaTemplate;
    

    @Autowired
    private CqlSession cqlSession;
    
    @After
    public void tearDown() {
    	eventCounsumedLatch = new CountDownLatch(1);
    }
    
    
	@Test
	public void test() {
		// GIVEN
		BS95Product bs95Product = mockExampleDTO();
		
        //WHEN
        sendToTopic("123", bs95Product);
        
        // THEN
        
    }
	
	@SneakyThrows
	protected void sendToTopic(String key, BS95Product bs95Product) {
		var record = new ProducerRecord<>(dlqTopic, key, bs95Product);
		kafkaTemplate.send(record).get();
		
		if(!eventCounsumedLatch.await(TIMEOUT, TimeUnit.SECONDS)) {
			throw new IllegalArgumentException("Timed Out");
		}
	}
	
	public BS95Product mockExampleDTO() {
		BS95Product bs95Product = new BS95Product();
		bs95Product.setCODPRODO("codprodo");
		bs95Product.setDESPRODO("desprodo");
		bs95Product.setEXECTYPE("exectype");
		bs95Product.setATCREATIONTIME("creationtime");
		bs95Product.setATCREATIONUSER("user");
		bs95Product.setATLASTMODIFIEDUSER("mduser");
		bs95Product.setATLASTMODIFIEDTIME("mdtime");
        return bs95Product;
    }


}

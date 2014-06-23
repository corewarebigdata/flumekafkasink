
package org.apache.flume.plugins;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventHelper;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

public class KafkaSink extends AbstractSink implements Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);

    private Properties parameters;
    
    private Producer<String, String> producer;
     
    private Context context;
     
    public void configure(Context context) {
        this.context = context;
        ImmutableMap<String, String> props = context.getParameters();

        parameters = new Properties();
        for (String key : props.keySet()) {
            String value = props.get(key);
            this.parameters.put(key, value);
        }
    }

    /**
     * Start void.
     */
    @Override
    public synchronized void start() {
        super.start();
        ProducerConfig config = new ProducerConfig(this.parameters);
        this.producer = new Producer<String, String>(config);
    }

    /**
     * Process status.
     * 
     * @return the status
     * @throws EventDeliveryException
     *             the event delivery exception
     */
    public Status process() throws EventDeliveryException {
        Status status = null;
        List<KeyedMessage<String, String>> messageList = new ArrayList<KeyedMessage<String, String>>();
        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        
        try {
        	
        	for(int i = 0 ;i<1000; i++){
	            Event event = ch.take();
	
	            String partitionKey = (String) parameters.get(KafkaFlumeConstans.PARTITION_KEY_NAME);
	            String encoding = StringUtils.defaultIfEmpty(
	                    (String) this.parameters.get(KafkaFlumeConstans.ENCODING_KEY_NAME),
	                    KafkaFlumeConstans.DEFAULT_ENCODING);
	            String topic = Preconditions.checkNotNull(
	                    (String) this.parameters.get(KafkaFlumeConstans.CUSTOME_TOPIC_KEY_NAME),
	                    "custom.topic.name is required");
	
	            String eventData = new String(event.getBody(), encoding);
	
	            KeyedMessage<String, String> data;
	
	            if (StringUtils.isEmpty(partitionKey)) {
	                data = new KeyedMessage<String, String>(topic, eventData);
	            } else {
	                data = new KeyedMessage<String, String>(topic, partitionKey, eventData);
	            }
	
	            if (LOGGER.isInfoEnabled()) {
	                LOGGER.info("Send Message to Kafka : [" + eventData + "] -- [" + EventHelper.dumpEvent(event) + "]");
	            }
	            messageList.add(data);
        	}
            producer.send(messageList);
            txn.commit();
        
            status = Status.READY;
       
            
            System.out.println("姣忔list鐨勫ぇ灏�+\t "+messageList.size());
        } catch (Throwable t) {
            txn.rollback();
            status = Status.BACKOFF;

            if (t instanceof Error) {
                throw (Error) t;
            }
        } finally {
            txn.close();
        }
        return status;
    }

    /**
     * Stop void.
     */
    @Override
    public void stop() {
        producer.close();
    }
}


package org.apache.flume.plugins;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.apache.flume.sink.AbstractSink;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

public class KafkaSink extends AbstractSink implements Configurable {


    private Properties parameters;
    
    private Producer producer;
     
    private Context context;
     
    
    @Override
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
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        List<KeyedMessage<String, Event>> messageList = new ArrayList<KeyedMessage<String, Event>>();
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
	            
	            Map<String,String> headMap = event.getHeaders();
	            
	            String souce_id = headMap.get(HeaderConstant.SOURCE_ID);
	            String source_type = headMap.get(HeaderConstant.SOURCE_TYPE);
	            String source_msg = headMap.get(HeaderConstant.SOURCE_MSG);
	            String source_name = headMap.get(HeaderConstant.SOURCE_NAME);
	            
	            topic = souce_id + source_type;
	
	            //String eventData = new String(event.getBody(), encoding);
	
	            KeyedMessage<String, Event> data;
	            if (StringUtils.isEmpty(partitionKey)) {
	                data = new KeyedMessage<String, Event>(topic, event);
	            } else {
	                data = new KeyedMessage<String, Event>(topic, partitionKey, event);
	            }
	
	            messageList.add(data);
        	}
            producer.send(messageList);
            txn.commit();
        
            status = Status.READY;
       
            
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

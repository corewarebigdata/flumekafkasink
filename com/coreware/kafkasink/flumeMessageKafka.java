package com.coreware.kafkasink;

import java.util.ArrayList;
import java.util.Properties;

import kafka.producer.Producer;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import scala.collection.Seq;

import com.google.common.collect.ImmutableMap;

public class flumeMessageKafka extends AbstractSink implements Configurable{

	
	/**
     * The Parameters.
     */
    private Properties parameters;
    /**
     * The Producer.
     */
    private Producer producer;
    /**
     * The Context.
     */
    private Context context;
	
	@Override
	public Status process() throws EventDeliveryException {
		ArrayList a = new ArrayList();
		producer.send((Seq) a);
		return null;
	}

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

}

package org.apache.flume.plugins;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableMap;

public class FlumeKafkaSink extends AbstractSink
  implements Configurable
{
  private static final Logger logger = Logger.getLogger(FlumeKafkaSink.class);
  private Producer<String, String> producer;
  private Properties props;
  private int batchSize;
  private String topic;
  
  @Override
  public void configure(Context context) {
    this.props = new Properties();
    ImmutableMap<String,String> map = context.getParameters();
    
    logger.debug("param map: " + map);
    
    for (String s : map.keySet()) {
      logger.debug("key: " + s + " value: " + (String)map.get(s));
      this.props.put(s, map.get(s));
    }
    
    this.batchSize = Integer.valueOf((String)this.props.get("custom.batch-size")).intValue()<1000?1000:500;
    this.topic = this.props.getProperty("topic");

    logger.info("configure obj and props is: " + this.props);
  }

  public Sink.Status process() throws EventDeliveryException {
	  
    logger.debug("process method is invoked");
    Channel channel = getChannel();
    Transaction trans = channel.getTransaction();
    Sink.Status status = Sink.Status.READY;

    trans.begin();
    // 鎵归噺鍙戦�list
    List<KeyedMessage<String,String>> list = new ArrayList<KeyedMessage<String,String>>();
    
    // 浠巆hannel鎷挎暟鎹�
    for (int i = 0; i < this.batchSize; i++) {
      
      Event event = channel.take();
      if (event == null) {
        status = Sink.Status.BACKOFF;
        break;
      }
      
      try {
    	  
    	  String body = new String(event.getBody(), "UTF-8");
    	  String header = event.getHeaders().toString();
    	  KeyedMessage<String,String> msg = new KeyedMessage<String,String>(this.topic, header, body);
    	  list.add(msg);
      }
      catch (UnsupportedEncodingException e) {
        logger.error(e);
      }
    }
    
    //灏嗘嬁鍒扮殑鏁版嵁鍙戦�鍒発afka
    try {
    	
      logger.debug("send data: " + list);
      this.producer.send(list);
      trans.commit();
    } catch (Exception e) {
    	
      logger.error("tanscation rollback for: " + e);
      trans.rollback();
    } catch (Throwable t) {
    	
      logger.error("unknown error: ", t);
      throw new RuntimeException(t);
    } finally {
    	
      trans.close();
    }
    
    //杩旈�鍙戦�鎴愬姛鏍囧織
    return status;
  }

  public synchronized void start() {
	  
    super.start();
    ProducerConfig config = new ProducerConfig(this.props);
    this.producer = new Producer<String, String>(config);
    
  }

  public synchronized void stop() {
    super.stop();
    this.producer.close();
  }
}
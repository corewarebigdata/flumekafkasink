package org.apache.flume.plugins;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;

import com.google.common.collect.ImmutableMap;

public class KafkaSource extends AbstractSource implements EventDrivenSource, Configurable {

  private Properties parameters;
  private Context context;
  private ConsumerConnector consumerConnector;
  private ExecutorService executorService;
  private SourceCounter sourceCounter;

  public void configure(Context context) {
	
    this.context = context;
    ImmutableMap<String,String> props = context.getParameters();

    this.parameters = new Properties();
    
    for (String key : props.keySet()) {

      String value = (String)props.get(key);
      this.parameters.put(key, value);
    }

    if (this.sourceCounter == null)
      this.sourceCounter = new SourceCounter(getName());
  }

  public synchronized void start() {

    super.start();
    this.sourceCounter.start();

    ConsumerConfig consumerConfig = new ConsumerConfig(this.parameters);
    this.consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

    Map topicCountMap = new HashMap();

    String topic = (String)this.parameters.get("custom.topic.name");
    String threadCount = (String)this.parameters.get("custom.thread.per.consumer");

    topicCountMap.put(topic, new Integer(threadCount));

    Map consumerMap = this.consumerConnector.createMessageStreams(topicCountMap);

    List<KafkaStream> streams = (List)consumerMap.get(topic);

    this.executorService = Executors.newFixedThreadPool(Integer.parseInt(threadCount));

    int tNumber = streams.size();
    
    for (KafkaStream stream : streams) {
      this.executorService.submit(new ConsumerWorker(stream, tNumber, this.sourceCounter));
    }
  }

  public synchronized void stop()
  {
    try
    {
      shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
    super.stop();
    this.sourceCounter.stop();

    ObjectName objName = null;
    try {
      objName = new ObjectName("org.apache.flume.source:type=" + getName());

      ManagementFactory.getPlatformMBeanServer().unregisterMBean(objName);
    } catch (Exception ex) {
      System.out.println("Failed to unregister the monitored counter: " + objName + ex.getMessage());
    }
  }

  private void shutdown()
    throws Exception
  {
    if (this.consumerConnector != null) {
      this.consumerConnector.shutdown();
    }

    if (this.executorService != null) {
      this.executorService.shutdown();
    }

    this.executorService.awaitTermination(9223372036854775807L, TimeUnit.SECONDS);
  }

  private class ConsumerWorker
    implements Runnable
  {
    private KafkaStream kafkaStream;
    private int threadNumber;
    private SourceCounter srcCount;
    
    private LinkedList kafkaconsumerList ;

    public ConsumerWorker(KafkaStream kafkaStream, int threadNumber, SourceCounter srcCount)
    {
      this.kafkaStream = kafkaStream;
      this.threadNumber = threadNumber;
      this.srcCount = srcCount;
      this.kafkaconsumerList = kafkaconsumerList;
    }

    public void run() {
      ConsumerIterator it = this.kafkaStream.iterator();
      try {
        while (it.hasNext()) {
          byte[] message = (byte[])it.next().message();
          Event event = EventBuilder.withBody(message);
          kafkaconsumerList.add(event);
          this.srcCount.incrementEventAcceptedCount();
        }
        KafkaSource.this.getChannelProcessor().processEventBatch(kafkaconsumerList);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}
package org.apache.flume.plugins;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinglePartition implements Partitioner {



    public SinglePartition(VerifiableProperties props) {
    }


	public int partition(Object arg0, int arg1)
	{
		return 0;
	}

}

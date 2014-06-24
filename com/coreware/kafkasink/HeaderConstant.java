package org.apache.flume.plugins;

public class HeaderConstant {
	
	
	// flume send  constant 
	public static final String AGENT_TIMESTAMP = "agent_timestamp ";
	public static final String SOURCE_HOST = "source_host ";
	public static final String SOURCE_TIMESTAMP = "source_timestamp";
	public static final String SOURCE_ORIGINAL_TIME = "source_original_time";
	public static final String SOURCE_MSG = "source_msg";
	public static final String SOURCE_NAME = "source_name ";
	public static final String SOURCE_TYPE = "source_type ";
	public static final String SOURCE_ID = "source_id";
	
	
	
	

}



/*
 * 
 * 
 * agent_timestamp ： 代理从源获取到数据时的时间戳
source_host：源数据的主机名
source_timestamp：源数据中数据的时间戳（经转换以后的）
source_original_time：源数据中原始的时间
source_msg：数据源消息
source_name：数据源名称	
source_type:日志类型
source_id: 应用ID
林文斌
2014/6/24 15:44:52
以 souce_id + source_type 作为topic.

souce_id + "_"+source_type

林文斌
source name, source id, sourcetype,source_msg 是从app过来的。

 source timestamp
**/

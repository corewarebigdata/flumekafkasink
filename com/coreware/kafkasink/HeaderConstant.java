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
 * agent_timestamp �� �����Դ��ȡ������ʱ��ʱ���
source_host��Դ���ݵ�������
source_timestamp��Դ���������ݵ�ʱ�������ת���Ժ�ģ�
source_original_time��Դ������ԭʼ��ʱ��
source_msg������Դ��Ϣ
source_name������Դ����	
source_type:��־����
source_id: Ӧ��ID
���ı�
2014/6/24 15:44:52
�� souce_id + source_type ��Ϊtopic.

souce_id + "_"+source_type

���ı�
source name, source id, sourcetype,source_msg �Ǵ�app�����ġ�

 source timestamp
**/

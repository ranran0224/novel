package com.my.hbase;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.thrift.generated.Hbase.Processor.scannerClose;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.crypto.provider.RSACipher;

public class HbaseDemo {
	public static String TN = "phone";
	public Random r = new Random();
	HBaseAdmin hAdmin;
	HTable hTable;
	
	@Before
	public void before() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		//step1 创建cofiguration
		Configuration conf = new Configuration();
		//集群就写集群ips，伪集群就写一个
		conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
		//step2 创建hbaseadmin、htable
		hAdmin = new HBaseAdmin(conf);
		hTable = new HTable(conf, TN);
	}
	
	@After
	public void after() throws IOException  {
		//关闭资源
		if(hAdmin != null){
			hAdmin.close();
		}
		if(hTable != null){
			hTable.close();
		}
	}
	/**
	 * 创建表
	 */
	public void createTable() throws IOException{
		//step1 判断表是否存在，存在删除
		if(hAdmin.tableExists(TN)){
			hAdmin.disableTable(TN);
			hAdmin.deleteTable(TN);
		}
		//step2 创建table description
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TN));
		
		//step3 创建cf description
		HColumnDescriptor cf = new HColumnDescriptor("cf");
		cf.setInMemory(true);
		cf.setMaxVersions(1);
		
		desc.addFamily(cf);
		//step4 创建表
		hAdmin.createTable(desc);
	}
	/**
	 * put
	 */
	public void put() throws RetriesExhaustedWithDetailsException, InterruptedIOException{
		String rowkey = "13910043825_20170426102955";
		Put put = new Put(rowkey.getBytes());
		put.add("cf".getBytes(), "type".getBytes(), "1".getBytes());
		hTable.put(put);
	}
	public void get() throws IOException{
		//查询条件
		String rowkey = "13910043825_20170426102955";
		Get get = new Get(rowkey.getBytes());
		get.addColumn("cf".getBytes(), "type".getBytes());
		//获取result
		Result result = hTable.get(get);
		Cell cell =result.getColumnLatestCell("cf".getBytes(), "type".getBytes());
		System.out.println("===============output==============================");
		System.out.println(new String(CellUtil.cloneValue(cell)));
	}
	/**
	 * 查询某个手机号 18694329803 所有的主叫类型 通话详单  type=1
	 * @throws IOException 
	 */
	public void scan() throws IOException{
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
		PrefixFilter prefixFilter = new PrefixFilter("18694329803".getBytes());
		filterList.addFilter(prefixFilter);
		
		SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter("cf".getBytes(), "type".getBytes(), CompareOp.EQUAL, "1".getBytes());
		filterList.addFilter(columnValueFilter);
		
		Scan scan = new Scan();
		scan.setFilter(filterList);
		
		ResultScanner resultScanner = hTable.getScanner(scan);
		for (Result result : resultScanner) {
			System.out.println("========================================================");
			System.out.println(new String(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
			System.out.println(new String(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
			System.out.println(new String(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
		}
	}
	/**
	 * 造数据
	 */
	public void insertDB() throws Exception {
		List<Put> puts = new ArrayList<Put>();
		for (int i = 0; i < 10; i++) {
			
			// 自己的手机号
			String pNum = getPhone("186");
			
			for (int j = 0; j < 100; j++) {
				// 目标手机号
				String dNum = getPhone("177");
				// 时间
				String dateStr = getDate("2017");
				
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
				
				String rowkey = pNum + "_" + (Long.MAX_VALUE-sdf.parse(dateStr).getTime());
				
				Put put = new Put(rowkey.getBytes());
				put.add("cf".getBytes(), "dnum".getBytes(), dNum.getBytes());
				put.add("cf".getBytes(), "date".getBytes(), dateStr.getBytes());
				put.add("cf".getBytes(), "type".getBytes(), (""+r.nextInt(2)).getBytes());
				
				puts.add(put);
			}
		}
		hTable.put(puts);
	}
	/**
	 * 随机返回手机号码
	 * @param prefix 手机号码前缀 eq：186
	 * @return 手机号码18612341234
	 */
	public String getPhone(String prefix) {
		return prefix + String.format("%08d", r.nextInt(99999999));
	}
	
	/**
	 * 随机返回日期 yyyyMMddHHmmss
	 * @param year 年
	 * @return 日期 20160101020203
	 */
	public String getDate(String year) {
		return year + String.format("%02d%02d%02d%02d%02d", 
				new Object[]{r.nextInt(12)+1,r.nextInt(29),
			r.nextInt(24),r.nextInt(24),r.nextInt(24)});
	}
}

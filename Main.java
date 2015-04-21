
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class Main{


	public static void main(String[] args){

		Configuration conf = HBaseConfiguration.create();
		try {
			long start = new Date().getTime();
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("raw"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("time"));
			tableDescriptor.addFamily(new HColumnDescriptor("price"));
			if ( admin.isTableAvailable("raw")){
				admin.disableTable("raw");
				admin.deleteTable("raw");
			}
			admin.createTable(tableDescriptor);
			
			//creating table 2 to store xi
			tableDescriptor = new HTableDescriptor(TableName.valueOf("xi_values"));
			tableDescriptor.addFamily(new HColumnDescriptor("Value"));	
			if ( admin.isTableAvailable("xi_values")){
				admin.disableTable("xi_values");
				admin.deleteTable("xi_values");
			}
			admin.createTable(tableDescriptor);
			
			//creating table 3 to store volatility
			tableDescriptor = new HTableDescriptor(TableName.valueOf("volatility"));
			tableDescriptor.addFamily(new HColumnDescriptor("Value"));	
			if ( admin.isTableAvailable("volatility")){
				admin.disableTable("volatility");
				admin.deleteTable("volatility");
			}
			admin.createTable(tableDescriptor);
			
			//creating table 4 to store top10 and bot10
			tableDescriptor = new HTableDescriptor(TableName.valueOf("top_bot"));
			tableDescriptor.addFamily(new HColumnDescriptor("Value"));	
			if ( admin.isTableAvailable("top_bot")){
				admin.disableTable("top_bot");
				admin.deleteTable("top_bot");
			}
			admin.createTable(tableDescriptor);


			Job job = Job.getInstance();
			job.setJarByClass(Main.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(Job1.Map.class);
			TableMapReduceUtil.initTableReducerJob("raw", null, job);
			job.setNumReduceTasks(0);
			job.waitForCompletion(true);
			
		
		
			
			Job job2 = Job.getInstance(conf);
			job2.setJarByClass(Main.class);
			Scan scan = new Scan();
			TableMapReduceUtil.initTableMapperJob("raw", scan, Job2.Map.class, Text.class, Text.class, job2);			
			TableMapReduceUtil.initTableReducerJob("xi_values", Job2.Reduce.class, job2);
			job2.setNumReduceTasks('1');
			job2.waitForCompletion( true );
			
			Job job3 = Job.getInstance(conf);
			job3.setJarByClass(Main.class);
			Scan scan3 = new Scan();
			TableMapReduceUtil.initTableMapperJob("xi_values", scan3, Job3.Map.class, Text.class, Text.class, job3);			
			TableMapReduceUtil.initTableReducerJob("volatility", Job3.Reduce.class, job3);
			job3.setNumReduceTasks('1');
			job3.waitForCompletion( true );
			
			Job job4 = Job.getInstance(conf);
			job4.setJarByClass(Main.class);
			Scan scan4 = new Scan();
			TableMapReduceUtil.initTableMapperJob("volatility", scan4, Job4.Map.class, Text.class, Text.class, job4);			
			TableMapReduceUtil.initTableReducerJob("top_bot", Job4.Reduce.class, job4);
			//job4.setNumReduceTasks('1');
			job4.waitForCompletion( true );
		
			admin.close();
			long end = new Date().getTime();
			System.out.println("The time taken is " + (end-start)/1000 + " Seconds");
		
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}



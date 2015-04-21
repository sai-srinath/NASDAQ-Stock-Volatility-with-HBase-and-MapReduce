import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
public class Job3{
	
	public static class Map extends TableMapper<Text, Text>{
		
		public void map(ImmutableBytesWritable rowkey, Result columns, Context context){
		
			//ImmutableBytesWritable key=new ImmutableBytesWritable(rowkey.get(),0,Bytes.SIZEOF_INT);
			//String k=rowkey.get().toString().substring(0,rowkey.toString().length()-6);
			
			String xi =Bytes.toString(columns.getValue(Bytes.toBytes("Value"),Bytes.toBytes("volatility")));
			String filename =Bytes.toString(columns.getValue(Bytes.toBytes("Value"),Bytes.toBytes("filename")));
			filename=filename.substring(0, filename.length()-6);
			
			try {
				//System.out.println("In job3 Mapper!"+filename+":"+xi);
				context.write(new Text(filename),new Text(xi));
			} catch (IOException e) {
				e.printStackTrace();
			}catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
public static class Reduce extends TableReducer< Text, Text, ImmutableBytesWritable> {
		
		public void reduce(Text key, Iterable<Text> values, Context context){
			
			double avg=0;
			double sum=0;
			double volatility=0;
			List<Double> xi = new ArrayList<Double>();
			
			for(Text value:values) {
				xi.add(Double.parseDouble(value.toString()));}
			for(int i=0;i<xi.size();i++) 
				{avg=(avg+xi.get(i));}
			
			avg=avg/(double)xi.size();
			for(int j=0;j<xi.size();j++) {
				double diff=xi.get(j)-avg;
				double square=Math.pow(diff, 2);
				sum=sum+square;}
			
			volatility=Math.sqrt(sum/(xi.size()-1));
			
			byte[] rowid = Bytes.toBytes(key.toString());
			Put p = new Put(rowid);
			p.add(Bytes.toBytes("Value"),Bytes.toBytes("volatility"),Bytes.toBytes(String.valueOf(volatility)));
			p.add(Bytes.toBytes("Value"),Bytes.toBytes("name"),rowid);
			try {
				//System.out.println("In Job3 reducer");
				context.write(new ImmutableBytesWritable(rowid), p);
				
			} catch (IOException e) {				
				e.printStackTrace();
			} catch (InterruptedException e) {				
				e.printStackTrace();
			}
			}
		}
		
	
	
	
	
}
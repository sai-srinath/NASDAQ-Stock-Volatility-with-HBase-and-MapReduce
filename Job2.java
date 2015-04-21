import java.io.IOException;
import java.util.HashMap;

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
public class Job2{
	
	public static class Map extends TableMapper<Text, Text>{
		
		
		public void map(ImmutableBytesWritable rowkey, Result columns, Context context){
			
			ImmutableBytesWritable key=new ImmutableBytesWritable(rowkey.get(),0,Bytes.SIZEOF_INT);
			
			
			
			//byte[] rowid=Bytes.toBytes(kset[0]+kset[1]+kset[2]);
			String filename =Bytes.toString( columns.getValue(Bytes.toBytes("stock"),Bytes.toBytes("name")));
			String day = Bytes.toString(columns.getValue(Bytes.toBytes("time"),Bytes.toBytes("dd")));
			String month = Bytes.toString(columns.getValue(Bytes.toBytes("time"),Bytes.toBytes("mm")));
			String year =Bytes.toString( columns.getValue(Bytes.toBytes("time"),Bytes.toBytes("yr")));
			String price =Bytes.toString( columns.getValue(Bytes.toBytes("price"),Bytes.toBytes("price")));
			
			String key1=filename+month+year;
			
			String value1=day+","+price;
			
			
			try {
				//System.out.println("In job2 Mapper!"+key1+":"+value1);
				context.write(new Text(key1),new Text(value1));
			} catch (IOException e) {
				e.printStackTrace();
			}catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
	
	public static class Reduce extends TableReducer< Text, Text, ImmutableBytesWritable> {
		
		public void reduce(Text key, Iterable<Text> values, Context context){
			//System.out.println("In job2 Reducer!");
			HashMap<Integer, Double> map = new HashMap<Integer, Double>();
			 for(Text value:values){
					String string = value.toString();
					String slist[]=string.split(",");
					map.put(Integer.parseInt(slist[0]),Double.parseDouble(slist[1]));
				}
			    int max=0;
				int min=50;
				double x=0;
				for(int i:map.keySet()){
					if(max<i)
				     max=i; }
				  
				for(int i:map.keySet()){
					if(min>i)
				     min=i; }
				x=(map.get(max)-map.get(min))/map.get(min);
				
				byte[] rowid = Bytes.toBytes(key.toString());
				Put p = new Put(rowid);
				p.add(Bytes.toBytes("Value"),Bytes.toBytes("volatility"),Bytes.toBytes(String.valueOf(x)));
				p.add(Bytes.toBytes("Value"),Bytes.toBytes("filename"),rowid);
				
				try {
					context.write(new ImmutableBytesWritable(rowid), p);
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		}
		
	}
	
	
	
}
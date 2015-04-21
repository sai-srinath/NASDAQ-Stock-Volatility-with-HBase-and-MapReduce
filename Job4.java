import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

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
public class Job4{
	
	public static class Map extends TableMapper<Text, Text>{
		
		public void map(ImmutableBytesWritable rowkey, Result columns, Context context){
			
			String volatility =Bytes.toString(columns.getValue(Bytes.toBytes("Value"),Bytes.toBytes("volatility")));
			String name =Bytes.toString(columns.getValue(Bytes.toBytes("Value"),Bytes.toBytes("name")));
			
			try {
				//System.out.println("In Job4 mapper with name:"+name);
				context.write(new Text(" "),new Text(name+"_"+volatility));
				
			} catch (IOException e) {				
				e.printStackTrace();
			} catch (InterruptedException e) {				
				e.printStackTrace();
			}
			
			
			
		}
	}
	
	
public static class Reduce extends TableReducer< Text, Text, ImmutableBytesWritable> {
		
	static TreeMap<Double,Text> tmap = new TreeMap<Double,Text>();
	 Text v = new Text();
	 static int count=0;
		
		public void reduce(Text key, Iterable<Text> values, Context context){
		
			//System.out.println("Entering Reduce4");
			for(Text value:values){
	            String s=value.toString();
	            String[] kv=s.split("_");
	            if(kv[0]!=null && kv[1]!=null){
	                
	    			tmap.put(Double.parseDouble(kv[1]),new Text(kv[0]));}
	             
	    		}
			System.out.println("Bottom 10 Stocks:");
			for(Double i:tmap.keySet()){
				
					
				//System.out.println(count);
				//v.set(String.valueOf(i));
				
				System.out.println("Stockname:"+tmap.get(i)+" Volatility: "+i);
				byte[] rowid = Bytes.toBytes(tmap.get(i).toString());
				Put p = new Put(rowid);
				p.add(Bytes.toBytes("Value"),Bytes.toBytes("volatility"),Bytes.toBytes(String.valueOf(i)));
				try {
					context.write(new ImmutableBytesWritable(rowid),p);
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			    count++;
			    if(count>=10)
			    	break; 
				
			  }
			
			count=0;
			System.out.println("Top 10 Stocks:");
			for(Double i:tmap.descendingKeySet()){
				
				
				//System.out.println(count);
				//v.set(String.valueOf(i));
				
				System.out.println("Stockname:"+tmap.get(i)+" Volatility: "+i);
				byte[] rowid = Bytes.toBytes(tmap.get(i).toString());
				Put p = new Put(rowid);
				p.add(Bytes.toBytes("Value"),Bytes.toBytes("volatility"),Bytes.toBytes(String.valueOf(i)));
				try {
					context.write(new ImmutableBytesWritable(rowid),p);
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			    count++;
			    if(count>=10)
			    	break; 
				
			  }
}
}
}

	
		
package cn.qzt360.Spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.qzt360.imsi.ImsiInfoClass;
import com.qzt360.imsi.ImsiLocation;

public class GetCityCodeService {
	
	public static Configuration conf = new Configuration();
	public static HashMap<Object, String> initImsiLocation() throws IOException{
		
		HashMap<Object, String> codeMap = new HashMap<Object, String>();
		BufferedReader br = null;
		FileSystem fs = FileSystem.newInstance(conf);
		// 构造BufferedReader对象
		try {
			br = new BufferedReader(new InputStreamReader(fs.open(new Path("/user/hujw/imsi.txt"))));//D:\\data\\imsi\\   ./imsi.txt
			String line = null;
			while ((line = br.readLine()) != null)
			{
				// 将文本打印到控制台
				String[] segs = line.split("\t");
				codeMap.put(segs[0], segs[1]+","+segs[5]);
			}
			System.out.println("initImsiLocation Success "+codeMap.size());
		} catch (Exception e) {
			e.printStackTrace();
		}finally
		{
			// 关闭BufferedReader
			if (br != null)
			{
				try
				{
					br.close();
				}catch (Exception e)
				{
				}
			}
			fs.close();
		}
		return codeMap;
	}
	
	
	public static String getLocationByImsi(String strImsi,HashMap<Object, String> codeMap) throws Exception{
		
		String info = "null,000000,null";
		
		try {
			System.out.println("imsi="+strImsi);
			System.out.println("codeMapSize ="+codeMap.size());
			ImsiLocation il = new ImsiLocation();
			ImsiInfoClass imsiInfo = il.getLocationByImsi(strImsi, codeMap);
			String cityCode = imsiInfo.getCitycode();
			String strPhoneNum = imsiInfo.getPhonenum();
			if (null != cityCode && !cityCode.contains("000000")) {
				
				info =  cityCode+","+strPhoneNum;
			}else {
				
				info =  "null,000000,null";
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return info;
	}
	
	
	/*public static void main(String[] args) {
		
		
		//System.out.println(FuncUtil.Long2StrTime(1481472047l*1000, "yyyyMMdd"));
		
		initImsiLocation();
		String info = GetLocationByImsi("460028869611481");
		System.out.println(info);
		System.out.println(info.split(",")[0]);
		System.out.println(info.split(",")[1]);
		System.out.println(info.split(",")[2]);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("ss", info.split(",")[0]);
		map.put("aa", info.split(",")[1]);
		map.put("cc", info.split(",")[2]);
		System.out.println(map.get("cc"));
		String strLine = "";
		long total = 0l;
		long error = 0l;
		long key = 0l;
		BufferedReader br = null;
		try {
			
			br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("./jingxin_107609.ok")), "UTF-8"));
			
			while((strLine = br.readLine())!=null){
				
				if (strLine.length() > 0) {
					
					String cityCode = GetLocationByImsi(strLine.split("\t")[1]);
					
					System.out.println(cityCode.split(",")[0]);
					
					System.out.println(cityCode.split(",")[1]);
					
					System.out.println(cityCode.split(",")[2]);
				}
				
			}
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("err Line   "+strLine);
		}finally{
			
			try {
				br.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}*/
}

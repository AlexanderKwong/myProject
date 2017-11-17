import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.intervalLiteral_return;

import scala.Tuple2;
import StructData.StaticConfig;
import mroxdrmerge.LocModel;
import mroxdrmerge.MainModel;

public class LocTestFunc
{

	public static void main(String[] args) throws Exception
	{
		{
			LocModel.GetInstance("123456789mastercom");
			
			ParseItem parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("OTT-LOC");
			if(parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			DataAdapterReader dataAdapterReader = new DataAdapterReader(parseItem);
			
			
			List<Tuple2<Long, String>> resList = new ArrayList<Tuple2<Long, String>>();
			
		    String xmString = "1493010164382	1493010164440	108546082	loc.map.baidu.com	2820D01	{\"content\":{\"bldg\":\"\",\"clf\":\"107.398427(29.700434(2000.000000\",\"floor\":\"\",\"indoor\":\"1\",\"point\":{\"x\":\"107.394277\",\"y\":\"29.702892\"},\"radius\":\"40.000000\"},\"result\":{\"error\":\"161\",\"time\":\"2017-04-24 13:02:44\"}}	460027162761368";
			String[] valstrs = xmString.toString().split(parseItem.getSplitMark(), -1);

			dataAdapterReader.readData(valstrs);
			
			long imsi_long = dataAdapterReader.GetLongValue("IMSI", -1);
			long ECI = dataAdapterReader.GetLongValue("ECI", -1);
			
			Date d_beginTime = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970,1,1)); 		
			int stime = (int) (d_beginTime.getTime() / 1000L);
			
			d_beginTime = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970,1,1)); 		
			int etime = (int) (d_beginTime.getTime() / 1000L);
			
			String host = dataAdapterReader.GetStrValue("host", "");
			String URI = dataAdapterReader.GetStrValue("URI", "");	
			String ul_content = dataAdapterReader.GetStrValue("ul_content", "");		
			String dl_content = dataAdapterReader.GetStrValue("dl_content", "");
			
			List<String> filledLocationInfoList = null;
			
//			filledLocationInfoList = LocModel.DecryptLocString("POST", host, URI, dl_content, 
//					ul_content, true, false, stime+"", ECI+"", imsi_long+""); 
			
			for (String str : filledLocationInfoList)
			{
				System.out.println(str);
			}
			
		}
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		String host = "loc.map.baidu.com";
		String uri = "";
		String DownCountent = "";
		DownCountent = "{\"content\":{\"bldg\":\"\",\"floor\":\"\",\"indoor\":\"0\",\"loctp\":\"cl\",\"point\":{\"x\":\"106.567430\",\"y\":\"29.572859\"},\"radius\":\"100.000000\"},\"result\":{\"error\":\"161\",\"time\":\"2017-08-21 17:39:52\"}}";
		String UpCountent = "";
		//UpCountent = "626c6f633d36504b2d3961575f5f4b32726f66536875614734704f2d2d36722d6d752d66763772693135656678744f69787a39545232355f636a656e423334445167646548793572476c4d7963794d6a59773871457a34754c32504c73737137787662533835667578392d5331344b656872764f68764b767838657a396f665034703658697a596d55376561387a5a6131774f504334384352783458592d366949715f69475f396d69676f474f386478334c6e4530636973716343386764794e314958306961546c6e4b7a6471597763504e6a5a68644355796477645956466c51483141534333525141304e434567684a415534445377346445454d5246565943425268414d77394e517a4233627a73314f6d395f646a39674e4449346469747a4a484639667a5176666e567277304b434f44442e253743747025334433";
		Date d_beginTime = new Date();
		int ECI = 0;
		long IMSI = 460008388056856L;

		LocModel.GetInstance("123456789mastercom");
//		List<String> filledLocationInfoList = LocModel.DecryptLocString("POST", host, uri, DownCountent, UpCountent,
//				true, false, d_beginTime.getTime() + "", ECI + "", IMSI + "");

//		for (String str : filledLocationInfoList)
//		{
//			System.out.println(str);
//		}

	}

}

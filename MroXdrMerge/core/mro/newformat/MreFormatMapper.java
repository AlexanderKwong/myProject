package mro.newformat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import StructData.MroOrigData;
import StructData.StaticConfig;

public class MreFormatMapper
{
    //mre结构
	//beginTime	ENBId	UserLabel	CellId	MmeCode	MmeGroupId	MmeUeS1apId	EventType	Weight	LteScRSRP	LteNcRSRP	LteScRSRQ	LteNcRSRQ	LteScEarfcn	LteScPci	LteScCgi	LteNcEarfcn	LteNcPci	LteFddNcRSRP	LteFddNcRSRQ	LteFddNcEarfcn	LteFddNcPci	LteTddNcRSRP	LteTddNcRSRQ	LteTddNcEarfcn	LteTddNcPci	GsmNcellBcch	GsmNcellCarrierRSSI	GsmNcellNcc	GsmNcellBcc	TdsPccpchRSCP	TdsNcellUarfcn	TdsCellParameterId
	
	public static class MreMapper extends Mapper<Object, Text, Text, Text>
	{
        private String MmeUeS1apId = "";
        private String TimeStamp = "";
        private String eutrancellId = "";
        private Text keyText = new Text();
        private Text valueText = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valstrs = value.toString().split("\t", -1);
			
			try
			{
				if(valstrs.length == 33)
				{
					mreHW(valstrs, context);
				}
				else if(valstrs.length == 58)
				{
					long eci = Long.parseLong(valstrs[3]);
					if(eci > 256)
					{
						mreZX(valstrs, context);
					}
					else 
					{
						mreDT(valstrs, context);
					}
				}
				else 
				{
					//throw new IOException("data type is not define :" + value.toString());
				}
			}
			catch (Exception e)
			{
				// TODO: handle exception
			}

			
		}
		
		private void mreHW(String[] valstrs, Context context) throws IOException, InterruptedException
		{
			MmeUeS1apId = valstrs[6];
			TimeStamp = valstrs[0];
			//eutrancellId 格式为 ****-**
			eutrancellId = valstrs[1] + "-" +  valstrs[3];
			
			keyText.set(eutrancellId + MmeUeS1apId + TimeStamp);
			valueText.set(mreHW2mro(valstrs));
			context.write(keyText, valueText);
		}
		
	    private String mreHW2mro(String[] values)
	    {
	    	MroOrigData mroData = new MroOrigData();
	    	
	    	mroData.reportTime = values[0];
	    	mroData.startTime = values[0];
	    	mroData.endTime = values[0];
	    	mroData.TimeStamp = values[0];
	    	mroData.enbId = Integer.parseInt(values[1]);
	    	mroData.userLabel = values[2];
	    	mroData.eutrancellId = String.valueOf(Integer.parseInt(values[1])*256 + Integer.parseInt(values[3]));
	    	mroData.MmeCode = Integer.parseInt(values[4]);
	    	mroData.MmeGroupId = Integer.parseInt(values[5]);
	    	mroData.MmeUeS1apId = Long.parseLong(values[6]);
	    	mroData.userLabel = values[2] + "," + values[7];//暂用user label
	    	mroData.LteScRSRP =  values[9].length() > 0 ? Integer.parseInt(values[9]) : StaticConfig.Int_Abnormal;
	    	mroData.LteNcRSRP =  values[10].length() > 0 ? Integer.parseInt(values[10]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScRSRQ =  values[11].length() > 0 ? Integer.parseInt(values[11]) : StaticConfig.Int_Abnormal;
	    	mroData.LteNcRSRQ =  values[12].length() > 0 ? Integer.parseInt(values[12]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScEarfcn =  values[13].length() > 0 ? Integer.parseInt(values[13]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPci =  values[14].length() > 0 ? Integer.parseInt(values[14]) : StaticConfig.Int_Abnormal;
	    	mroData.LteNcEarfcn =  values[16].length() > 0 ? Integer.parseInt(values[16]) : StaticConfig.Int_Abnormal;
	    	mroData.LteNcPci =  values[17].length() > 0 ? Integer.parseInt(values[17]) : StaticConfig.Int_Abnormal;
	    	mroData.GsmNcellBcch =  values[26].length() > 0 ? Integer.parseInt(values[26]) : StaticConfig.Int_Abnormal;
	    	mroData.GsmNcellCarrierRSSI =  values[27].length() > 0 ? Integer.parseInt(values[27]) : StaticConfig.Int_Abnormal;
	    	mroData.GsmNcellNcc =  values[28].length() > 0 ? Integer.parseInt(values[28]) : StaticConfig.Int_Abnormal;
	    	mroData.GsmNcellBcc =  values[29].length() > 0 ? Integer.parseInt(values[29]) : StaticConfig.Int_Abnormal;
	    	mroData.TdsPccpchRSCP =  values[30].length() > 0 ? Integer.parseInt(values[30]) : StaticConfig.Int_Abnormal;
	    	mroData.TdsNcellUarfcn =  values[31].length() > 0 ? Integer.parseInt(values[31]) : StaticConfig.Int_Abnormal;
	    	
	    	return mroData.GetData();
	    }
	    
		private void mreZX(String[] valstrs, Context context) throws IOException, InterruptedException
		{	
			MmeUeS1apId = valstrs[8];
			TimeStamp = valstrs[0];
			//eutrancellId 格式为 ****-**
			long eci = Long.parseLong(valstrs[3]);
			eutrancellId = (eci/256) + "-" +  (eci%256);
			
			keyText.set(eutrancellId + MmeUeS1apId + TimeStamp);
			valueText.set(mreZX2mro(valstrs));
			context.write(keyText, valueText);
		}
		
	    private String mreZX2mro(String[] values)
	    {
	    	MroOrigData mroData = new MroOrigData();
	    	
	    	mroData.reportTime = values[0];
	    	mroData.startTime = values[0];
	    	mroData.endTime = values[0];
	    	mroData.TimeStamp = values[0];
	    	mroData.enbId = Integer.parseInt(values[1]);
	    	mroData.userLabel = values[2];
	    	mroData.eutrancellId = values[3];
	    	mroData.MmeCode = values[6].length() > 0 ? Integer.parseInt(values[6]) : StaticConfig.Int_Abnormal;
	    	mroData.MmeGroupId = values[7].length() > 0 ? Integer.parseInt(values[7]) : StaticConfig.Int_Abnormal;
	    	mroData.MmeUeS1apId = values[8].length() > 0 ? Integer.parseInt(values[8]) : StaticConfig.Int_Abnormal;
	    	mroData.userLabel = mroData.userLabel + "," + values[10];
	    	mroData.LteScRSRP = values[11].length() > 0 ? Integer.parseInt(values[11]) : StaticConfig.Int_Abnormal;
	    	mroData.LteNcRSRP = values[12].length() > 0 ? Integer.parseInt(values[12]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScRSRQ = values[13].length() > 0 ? Integer.parseInt(values[13]) : StaticConfig.Int_Abnormal;
	    	mroData.LteNcRSRQ = values[14].length() > 0 ? Integer.parseInt(values[14]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScEarfcn = values[15].length() > 0 ? Integer.parseInt(values[15]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPci = values[16].length() > 0 ? Integer.parseInt(values[16]) : StaticConfig.Int_Abnormal;
	    	mroData.LteNcEarfcn = values[17].length() > 0 ? Integer.parseInt(values[17]) : StaticConfig.Int_Abnormal;
	    	mroData.LteNcPci = values[18].length() > 0 ? Integer.parseInt(values[18]) : StaticConfig.Int_Abnormal;
	    	mroData.GsmNcellCarrierRSSI = values[19].length() > 0 ? Integer.parseInt(values[19]) : StaticConfig.Int_Abnormal;
	    	mroData.GsmNcellBcch = values[20].length() > 0 ? Integer.parseInt(values[20]) : StaticConfig.Int_Abnormal;
	    	mroData.GsmNcellNcc = values[21].length() > 0 ? Integer.parseInt(values[21]) : StaticConfig.Int_Abnormal;
	    	mroData.GsmNcellBcc = values[22].length() > 0 ? Integer.parseInt(values[22]) : StaticConfig.Int_Abnormal;
	    	mroData.TdsPccpchRSCP = values[23].length() > 0 ? Integer.parseInt(values[23]) : StaticConfig.Int_Abnormal;
	    	mroData.TdsNcellUarfcn = values[24].length() > 0 ? Integer.parseInt(values[24]) : StaticConfig.Int_Abnormal;
	    	mroData.TdsCellParameterId = values[25].length() > 0 ? Integer.parseInt(values[25]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScRTTD = values[27].length() > 0 ? Integer.parseInt(values[27]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScTadv = values[28].length() > 0 ? Integer.parseInt(values[28]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScAOA = values[29].length() > 0 ? Integer.parseInt(values[29]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPHR = values[30].length() > 0 ? Integer.parseInt(values[30]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScRIP = values[31].length() > 0 ? Integer.parseInt(values[31]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScSinrUL = values[32].length() > 0 ? Integer.parseInt(values[32]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci1 = values[33].length() > 0 ? Integer.parseInt(values[33]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci2 = values[34].length() > 0 ? Integer.parseInt(values[34]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci3 = values[35].length() > 0 ? Integer.parseInt(values[35]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci4 = values[36].length() > 0 ? Integer.parseInt(values[36]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci5 = values[37].length() > 0 ? Integer.parseInt(values[37]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci6 = values[38].length() > 0 ? Integer.parseInt(values[38]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci7 = values[39].length() > 0 ? Integer.parseInt(values[39]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci8 = values[40].length() > 0 ? Integer.parseInt(values[40]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci9 = values[41].length() > 0 ? Integer.parseInt(values[41]) : StaticConfig.Int_Abnormal;
	    	
	    	mroData.LteScPlrDLQci1 = values[42].length() > 0 ? Integer.parseInt(values[42]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci2 = values[43].length() > 0 ? Integer.parseInt(values[43]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci3 = values[44].length() > 0 ? Integer.parseInt(values[44]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci4 = values[45].length() > 0 ? Integer.parseInt(values[45]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci5 = values[46].length() > 0 ? Integer.parseInt(values[46]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci6 = values[47].length() > 0 ? Integer.parseInt(values[47]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci7 = values[48].length() > 0 ? Integer.parseInt(values[48]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci8 = values[49].length() > 0 ? Integer.parseInt(values[49]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci9 = values[50].length() > 0 ? Integer.parseInt(values[50]) : StaticConfig.Int_Abnormal;
	    	
	    	return mroData.GetData();
	    }
	    
		private void mreDT(String[] valstrs, Context context) throws IOException, InterruptedException
		{	
			MmeUeS1apId = valstrs[8];
			TimeStamp = valstrs[0];
			//eutrancellId 格式为 ****-**
			long eci = Long.parseLong(valstrs[1]) * 256 + Long.parseLong(valstrs[3]) ;
			eutrancellId = (eci/256) + "-" +  (eci%256);
			
			keyText.set(eutrancellId + MmeUeS1apId + TimeStamp);
			valueText.set(mreDT2mro(valstrs));
			context.write(keyText, valueText);
		}
		
	    private String mreDT2mro(String[] values)
	    {
	    	MroOrigData mroData = new MroOrigData();
	    	
	    	mroData.reportTime = values[0];
	    	mroData.startTime = values[0];
	    	mroData.endTime = values[0];
	    	mroData.TimeStamp = values[0];
	    	mroData.enbId = Integer.parseInt(values[1]);
	    	mroData.userLabel = values[2];
	    	mroData.eutrancellId = String.valueOf(Long.parseLong(values[1]) * 256 + Long.parseLong(values[3])) ;
	    	mroData.MmeCode = values[6].length() > 0 ? Integer.parseInt(values[6]) : StaticConfig.Int_Abnormal;
	    	mroData.MmeGroupId = values[7].length() > 0 ? Integer.parseInt(values[7]) : StaticConfig.Int_Abnormal;
	    	mroData.MmeUeS1apId = values[8].length() > 0 ? Integer.parseInt(values[8]) : StaticConfig.Int_Abnormal;
	    	mroData.userLabel = mroData.userLabel + "," + values[10];
	    	mroData.LteScRSRP = values[11].length() > 0 ? Integer.parseInt(values[11]) : StaticConfig.Int_Abnormal;
	    	mroData.LteNcRSRP = values[12].length() > 0 ? Integer.parseInt(values[12]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScRSRQ = values[13].length() > 0 ? Integer.parseInt(values[13]) : StaticConfig.Int_Abnormal;
	    	mroData.LteNcRSRQ = values[14].length() > 0 ? Integer.parseInt(values[14]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScEarfcn = values[15].length() > 0 ? Integer.parseInt(values[15]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPci = values[16].length() > 0 ? Integer.parseInt(values[16]) : StaticConfig.Int_Abnormal;
	    	mroData.LteNcEarfcn = values[17].length() > 0 ? Integer.parseInt(values[17]) : StaticConfig.Int_Abnormal;
	    	mroData.LteNcPci = values[18].length() > 0 ? Integer.parseInt(values[18]) : StaticConfig.Int_Abnormal;
	    	mroData.GsmNcellCarrierRSSI = values[19].length() > 0 ? Integer.parseInt(values[19]) : StaticConfig.Int_Abnormal;
	    	mroData.GsmNcellBcch = values[20].length() > 0 ? Integer.parseInt(values[20]) : StaticConfig.Int_Abnormal;
	    	mroData.GsmNcellNcc = values[21].length() > 0 ? Integer.parseInt(values[21]) : StaticConfig.Int_Abnormal;
	    	mroData.GsmNcellBcc = values[22].length() > 0 ? Integer.parseInt(values[22]) : StaticConfig.Int_Abnormal;
	    	mroData.TdsPccpchRSCP = values[23].length() > 0 ? Integer.parseInt(values[23]) : StaticConfig.Int_Abnormal;
	    	mroData.TdsNcellUarfcn = values[24].length() > 0 ? Integer.parseInt(values[24]) : StaticConfig.Int_Abnormal;
	    	mroData.TdsCellParameterId = values[25].length() > 0 ? Integer.parseInt(values[25]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScRTTD = values[27].length() > 0 ? Integer.parseInt(values[27]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScTadv = values[28].length() > 0 ? Integer.parseInt(values[28]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScAOA = values[29].length() > 0 ? Integer.parseInt(values[29]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPHR = values[30].length() > 0 ? Integer.parseInt(values[30]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScRIP = values[31].length() > 0 ? Integer.parseInt(values[31]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScSinrUL = values[32].length() > 0 ? Integer.parseInt(values[32]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci1 = values[33].length() > 0 ? Integer.parseInt(values[33]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci2 = values[34].length() > 0 ? Integer.parseInt(values[34]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci3 = values[35].length() > 0 ? Integer.parseInt(values[35]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci4 = values[36].length() > 0 ? Integer.parseInt(values[36]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci5 = values[37].length() > 0 ? Integer.parseInt(values[37]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci6 = values[38].length() > 0 ? Integer.parseInt(values[38]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci7 = values[39].length() > 0 ? Integer.parseInt(values[39]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci8 = values[40].length() > 0 ? Integer.parseInt(values[40]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrULQci9 = values[41].length() > 0 ? Integer.parseInt(values[41]) : StaticConfig.Int_Abnormal;
	    	
	    	mroData.LteScPlrDLQci1 = values[42].length() > 0 ? Integer.parseInt(values[42]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci2 = values[43].length() > 0 ? Integer.parseInt(values[43]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci3 = values[44].length() > 0 ? Integer.parseInt(values[44]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci4 = values[45].length() > 0 ? Integer.parseInt(values[45]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci5 = values[46].length() > 0 ? Integer.parseInt(values[46]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci6 = values[47].length() > 0 ? Integer.parseInt(values[47]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci7 = values[48].length() > 0 ? Integer.parseInt(values[48]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci8 = values[49].length() > 0 ? Integer.parseInt(values[49]) : StaticConfig.Int_Abnormal;
	    	mroData.LteScPlrDLQci9 = values[50].length() > 0 ? Integer.parseInt(values[50]) : StaticConfig.Int_Abnormal;
	    	
	    	return mroData.GetData();
	    }
	
	    

	    
	}
	

}

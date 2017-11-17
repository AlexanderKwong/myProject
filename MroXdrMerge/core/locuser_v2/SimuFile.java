package locuser_v2;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mroxdrmerge.MainModel;

public class SimuFile
{
    private String fname = "";
    private String fpath = "";

    public SimuFile(String sname, String spath)
    {
        fname = sname;
        fpath = spath;
    }
    
    public List<ArrayList<SimuGrid>> ReadFile(int cityid, int eci)
    {
        String flname = MainModel.GetInstance().getAppConfig().getSimuLocConfigPath() + "/" + String.valueOf(cityid) + "/" + fpath + "/FINGERPRINT_" + fname + "_" + String.valueOf(eci) + ".txt";

        BufferedReader sr = CfgInfo.getReader(flname, null);
        if (sr == null)
        {
        	return null;        	
        }

        List<ArrayList<SimuGrid>> sgs  = new ArrayList<ArrayList<SimuGrid>>();
        sgs.add(null);
        sgs.add(null);
        
		try
		{
			int i = 0;		
            String sline = sr.readLine();
            while (sline != null)
            {
            	String[] recs = sline.split("\t", -1);
                if (recs.length != 21)
                {
                    sline = sr.readLine();
                    continue;
                }

                i = 0;
                try
                {                
                    SimuGrid sg = new SimuGrid();
                    sg.eci = Integer.parseInt(recs[i++]);
                    sg.buildingid = Integer.parseInt(recs[i++]);
                    sg.longitude = Integer.parseInt(recs[i++]);
                    sg.latitude = Integer.parseInt(recs[i++]);
                    sg.level = Integer.parseInt(recs[i++]);
                    sg.rsrp = Double.parseDouble(recs[i++]);
                    sg.comeci1 = Integer.parseInt(recs[i++]);
                    sg.comeci1diff = Double.parseDouble(recs[i++]);
                    sg.comeci2 = Integer.parseInt(recs[i++]);
                    sg.comeci2diff = Double.parseDouble(recs[i++]);
                    sg.maxrsrp = Double.parseDouble(recs[i++]);
                    // 这里累加后面直接用
                    sg.arfcn = Integer.parseInt(recs[i++]);
                    sg.arfcnrsrp = Double.parseDouble(recs[i++]);
                    sg.num[0] = Integer.parseInt(recs[i++]);             //75 
                    sg.num[1] = Integer.parseInt(recs[i++]) + sg.num[0]; //80 
                    sg.num[2] = Integer.parseInt(recs[i++]) + sg.num[1]; //85 
                    sg.num[3] = Integer.parseInt(recs[i++]) + sg.num[2]; //90 
                    sg.num[4] = Integer.parseInt(recs[i++]) + sg.num[3]; //95 
                    sg.num[5] = Integer.parseInt(recs[i++]) + sg.num[4]; //100
                    sg.num[6] = Integer.parseInt(recs[i++]) + sg.num[5]; //105
                    sg.isspec = Integer.parseInt(recs[i++]);
                    
                    if (sg.buildingid > 0)
                    {
                    	int natte = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSimuInAtte());
                    	
                        sg.rsrp = sg.rsrp - natte;
                        sg.maxrsrp = sg.maxrsrp - natte;
                        
                        if (sgs.get(0) == null)
                        {
                            sgs.set(0, new ArrayList<SimuGrid>());
                        }
                        sgs.get(0).add(sg);
                    }
                    else
                    {
                        if (sgs.get(1) == null)
                        {
                            sgs.set(1, new ArrayList<SimuGrid>());
                        }
                        sgs.get(1).add(sg);
                    } 
                }
	            catch (Exception ee)
	            {                
	            }
	            sline = sr.readLine();
            }
        }
	    catch (Exception ee)
	    {
	    	sgs = null;
	    }

		if (sr != null)
		{
			try
			{
				sr.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}	
		
        return sgs;
    }
}

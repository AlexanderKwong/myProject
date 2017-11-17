package mro.lablefillex;

import cellconfig.CellBuildWifi;

public class WifiFixed
{
	public static int returnLevel(CellBuildWifi buildWifi, String wifiList, int buildid)
	{
		int level = -1;
		String macs[] = wifiList.split(",", -1);
		int wifisize = macs.length;
		Wifi wf = null;
		float weight = 0f;
		float denominator = 0f;// 分母
		float numerator = 0f;// 分子

		for (int i = 0; i < wifisize; i++)
		{
			wf = new Wifi();
			String temp[] = macs[i].split(";", -1);
			if (temp.length >= 2)
			{
				wf.mac = temp[0];
				wf.rsrp = Float.parseFloat(temp[1]) * -1;
				level = buildWifi.getLevel(buildid, wf.mac);
				if (level > 0)
				{
					wf.level = level;
					if (wf.rsrp >= -60)
					{
						weight = 1;
					}
					else if (wf.rsrp <= -100)
					{
						weight = 0;
					}
					else
					{
						weight = (wf.rsrp + 100) / 40;
					}
					numerator += wf.level * weight;
					denominator += weight;
				}
				else
				{
					continue;
				}
			}
		}

		if (denominator > 0)
		{
			return Math.round(numerator / denominator) / 3 * 3;
		}
		return -1;
	}

}

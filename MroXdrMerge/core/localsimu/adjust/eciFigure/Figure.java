package localsimu.adjust.eciFigure;

public class Figure
{
	public int buildingid;
	public int longtitude;
	public int latitude;
	public int level;
	public int eci;
	public int earthFn;
	public int pci;
	public double rsrp;

	public Figure(String value)
	{
		setValeu(value);
	}

	public void setValeu(String value)
	{
		String temp[] = value.split(",|\t", -1);
		buildingid = Integer.parseInt(temp[0]);
		longtitude = Integer.parseInt(temp[1]);
		latitude = Integer.parseInt(temp[2]);
		level = Integer.parseInt(temp[3]);
		eci = Integer.parseInt(temp[4]);
		earthFn = Integer.parseInt(temp[5]);
		pci = Integer.parseInt(temp[6]);
		rsrp = Double.parseDouble(temp[7]);
	}

	public String getContent()
	{
		String spliter = "\t";
		String content = buildingid + spliter + longtitude + spliter + latitude + spliter + level + spliter + eci
				+ spliter + earthFn + spliter + pci + spliter + rsrp;
		return content;
	}

}

package mro.lablefill_xdr_figure;

public class FigureCell
{
	private long eci;
	private double rsrp;
	private int earfcn;
	private int pci;
	private long gongcanIlongitude;// 对应公参的经度
	private long gongcanIlatitud;// 对应公参的维度

	public FigureCell(long eci, double rsrp, int earfcn, int pci, long gongcanIlongitude, long gongcanIlatitud)
	{
		this.eci = eci;
		this.rsrp = rsrp;
		this.earfcn = earfcn;
		this.pci = pci;
		this.gongcanIlongitude = gongcanIlongitude;
		this.gongcanIlatitud = gongcanIlatitud;
	}

	public long getEci()
	{
		return eci;
	}

	public void setEci(long eci)
	{
		this.eci = eci;
	}

	public double getRsrp()
	{
		return rsrp;
	}

	public void setRsrp(double rsrp)
	{
		this.rsrp = rsrp;
	}

	public int getEarfcn()
	{
		return earfcn;
	}

	public void setEarfcn(int earfcn)
	{
		this.earfcn = earfcn;
	}

	public int getPci()
	{
		return pci;
	}

	public void setPci(int pci)
	{
		this.pci = pci;
	}

	public long getGongcanIlongitude()
	{
		return gongcanIlongitude;
	}

	public void setGongcanIlongitude(long gongcanIlongitude)
	{
		this.gongcanIlongitude = gongcanIlongitude;
	}

	public long getGongcanIlatitud()
	{
		return gongcanIlatitud;
	}

	public void setGongcanIlatitud(long gongcanIlatitud)
	{
		this.gongcanIlatitud = gongcanIlatitud;
	}
	
	

}

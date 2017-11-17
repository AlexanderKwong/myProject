package localsimu.cellgrid_merge;

public class CellGridStruct
{
	public int itllongitude;
	public int itllatitude;
	public long iCi;
	public double rsrp;
	public static final String spliter = ",";

	public CellGridStruct(int itllongitude, int itllatitude, long iCi, double rsrp)
	{
		this.itllongitude = itllongitude;
		this.itllatitude = itllatitude;
		this.iCi = iCi;
		this.rsrp = rsrp;
	}

	@Override
	public String toString()
	{
		return itllongitude + spliter + itllatitude + spliter + iCi + spliter + rsrp;
	}
}

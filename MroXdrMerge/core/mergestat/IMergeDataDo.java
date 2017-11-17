package mergestat;

public interface IMergeDataDo
{
    public String getMapKey();
	public int getDataType();
	public int setDataType(int dataType);
	
	public boolean mergeData(Object o); 
	public boolean fillData(String[] vals, int sPos);
	public String getData();
	
	
}

import java.io.File;
import java.io.FileOutputStream;

import com.sun.org.apache.bcel.internal.generic.StackInstruction;

import jan.util.db.DBRow;
import jan.util.db.IFetchData;
import jan.util.db.SqlDBHelper;

public class LocSimuGrid
{

	public static void main(String[] args) throws Exception
	{
		LocSimuGrid locSimuGrid = new LocSimuGrid();
		locSimuGrid.makeConfig();
	}

	private StringBuffer sbData = new StringBuffer();
	private int templong = -1;
	private int templat = -1;
	private String tempCellList = "";
	private String tempRsrpList = "";
	private FileOutputStream fileOutputStream;

	public void makeConfig()
	{
		String sql = "select [ieci],[ilongitude],[ilatitude],[rsrp] from [tb_simu_result_coverface] order by ilongitude,ilatitude,rsrp";
		String dbConnStr = "jdbc:sqlserver://192.168.1.92:1433; databaseName=RAMS_HAERBIN; User=dtauser;Password=dtauser;";

		try
		{
			String path = "C:\\Users\\Jancan\\Desktop\\TB_CFG_LOCSIMU.txt";
			File file = new File(path);
			if (file.exists())
			{
				file.delete();
			}
			file.createNewFile();
			fileOutputStream = new FileOutputStream(file, true); // 如果追加方式用true

			SqlDBHelper.ExcuteQuery(dbConnStr, sql, new IFetchData()
			{
				@Override
				public boolean fetchData(DBRow row)
				{
					int eci;
					int rsrp;
					int longitude = -1;
					int latitude = -1;
					sbData.delete(0, sbData.length());

					eci = row.getInt(0);
					longitude = row.getInt(1);
					latitude = row.getInt(2);
					rsrp = (int) row.getDouble(3);

					if (longitude != templong || latitude != templat)
					{
						if (tempCellList.length() > 0)
						{
							sbData.append(templong);
							sbData.append("\t");
							sbData.append(templat);
							sbData.append("\t");
							sbData.append(tempCellList);
							sbData.append("\t");
							sbData.append(tempRsrpList);
							sbData.append("\n");

							try
							{
								fileOutputStream.write(sbData.toString().getBytes());
							}
							catch (Exception e)
							{
								// TODO: handle exception
							}
						}

						templong = longitude;
						templat = latitude;
						tempCellList = "";
						tempRsrpList = "";
					}

					tempCellList = tempCellList.length() > 0 ? "," + eci : "" + eci;
					tempRsrpList = tempRsrpList.length() > 0 ? "," + rsrp : "" + rsrp;

					return true;
				}
			});

			
			if (tempCellList.length() > 0)
			{
				sbData.append(templong);
				sbData.append("\t");
				sbData.append(templat);
				sbData.append("\t");
				sbData.append(tempCellList);
				sbData.append("\t");
				sbData.append(tempRsrpList);
				sbData.append("\n");

				try
				{
					fileOutputStream.write(sbData.toString().getBytes());
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
		}
		catch (Exception ex)
		{
			System.out.println(ex.getStackTrace());
		}
		finally
		{
			try
			{
				fileOutputStream.close();
			}
			catch (Exception e)
			{
				// TODO: handle exception
			}
		}

	}

}

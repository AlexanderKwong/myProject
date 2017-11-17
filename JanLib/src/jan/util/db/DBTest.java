package jan.util.db;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by Administrator on 2017/3/15.
 */
public class DBTest {
    public static void main(String[] args) throws SQLException {
        DBHelper dbHelper = new DBHelper();
        ResultSet resultSet = dbHelper.GetResultSet("SELECT top 10  * " +
                "FROM [MBD_SIMU_C_Shanghai].[dbo].[tb_area_list_point] " +
                "where iareatypeid = 1100 AND strareaname NOT LIKE '%ÎÞÃûÂ·%'", null);
        while (resultSet.next()){
            String strareaname = resultSet.getString("strareaname");
            System.out.println(strareaname);
        }

    }
}


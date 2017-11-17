package xdr.locallex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import xdr.locallex.model.HttpPage;
import xdr.locallex.model.XdrDataBase;
import xdr.locallex.model.XdrData_Http;

public class HttpPageDeal
{
	public static DataStater dataStater = null;

	public static ArrayList<EventData> deal(ArrayList<XdrDataBase> xdrDataBaseList)
	{

		ArrayList<EventData> eventDataListAll = new ArrayList<EventData>();

		HashMap<String, HttpPage> httpHuaDangMap = new HashMap<>();

		for (XdrDataBase xdrData : xdrDataBaseList)
		{
			XdrData_Http xdrData_Http = (XdrData_Http) xdrData;
			/****************************************************************************************************************/
			/****************************************************************************************************************/
			/**
			 * TODO zhaikaishun 2017-11-02 加一个transaction_type=6 contains
			 * 改成equals
			 */

			if (xdrData_Http.App_Type == 15 && xdrData_Http.HTTP_content_type.equals("text/html")
					&& !xdrData_Http.URI.equals("") && xdrData_Http.TRANSACTION_TYPE == 6)
			{
				if (httpHuaDangMap.containsKey(xdrData_Http.URI))
				{
					HttpPage httpPage = httpHuaDangMap.get(xdrData_Http.URI);

					if ((xdrData_Http.istime * 1000L + xdrData_Http.istimems - httpPage.Procedure_Start_Time) < 1000)
					{
						continue;
					}

					if (httpPage.合并话单数 > 5 && (httpPage.Procedure_End_Time - httpPage.Procedure_Start_Time) < 60000)
					{
						// 吐出
						ArrayList<EventData> eventDataList = httpPage.toEventData();
						eventDataListAll.addAll(eventDataList);

					}
					httpHuaDangMap.remove(xdrData_Http.URI);
				}
				// 加载进来1. new HttpHuaDang, 2. 加载数据,3.put(url,httpHuaDang)
				HttpPage huaPage = new HttpPage();
				huaPage.loadData(xdrData_Http);

				/**
				 * TODO 还需要判断元素的结束时间 需要大于 框架的开始时间,我这里框架的不判断了
				 */
				if (xdrData.ietime * 1000L + xdrData.ietimems > (xdrData.istime * 1000L + xdrData.istimems))
				{
					huaPage.statData(xdrData_Http);
				}

				httpHuaDangMap.put(xdrData_Http.URI, huaPage);
			}
			/**
			 * TODO zhaikaishun 之前只有else，现在加一个else if xdrData_Http.App_Type ==
			 * 15 && 这个去掉了
			 */
			// else
			else if (xdrData_Http.TRANSACTION_TYPE == 6)
			{
				if (!xdrData_Http.Refer_URI.equals("") && httpHuaDangMap.containsKey(xdrData_Http.Refer_URI))
				{

					HttpPage thisHuaDang = httpHuaDangMap.get(xdrData_Http.Refer_URI);
					if (xdrData_Http.Eci == 17155845)
					{
						System.out.println("uri: " + thisHuaDang.URI);
						System.out.println("ref_uri: " + xdrData_Http.Refer_URI);
					}

					// 1. 页面元素的开始时间-上一次的结束时间小于2000毫秒
					if (xdrData_Http.istime * 1000L + xdrData_Http.istimems - thisHuaDang.lastEndTime <= 2000)
					{
						// 页面元素的开始时间大于框架的结束时间
						if ((xdrData.istime * 1000L + xdrData.istimems) > thisHuaDang.a_Procedure_End_time)
						{
							thisHuaDang.statData(xdrData_Http);
						}
//						else
//						{
//							thisHuaDang.lastEndTime = xdrData_Http.ietime * 1000L + xdrData_Http.ietimems;
//						}

					}
//					else
//					{
//						thisHuaDang.lastEndTime = xdrData_Http.ietime * 1000L + xdrData_Http.ietimems;
//					}
				}
			}
		}

		for (String url : httpHuaDangMap.keySet())
		{
			HttpPage httpHuaDang = httpHuaDangMap.get(url);
			if (httpHuaDang.合并话单数 > 5 && (httpHuaDang.Procedure_End_Time - httpHuaDang.Procedure_Start_Time) < 60000)
			{
				// 吐出
				ArrayList<EventData> eventDataList = httpHuaDang.toEventData();
				eventDataListAll.addAll(eventDataList);
			}
		}
		httpHuaDangMap.clear();
		return eventDataListAll;

	}

}

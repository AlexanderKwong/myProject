package xdr.lablefill;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class LableItemMng
{
    private List<LableItem> lableItemList;
    
    public LableItemMng()
    {
    	lableItemList = new ArrayList<LableItem>();
    }
    
    public void AddItem(LableItem item)
    {
    	lableItemList.add(item);
    }
    
    public List<LableItem> getLableItemList()
	{
		return lableItemList;
	}
    
    public void init()
    {	
		Collections.sort(lableItemList, new Comparator<LableItem>()
		{
			public int compare(LableItem a, LableItem b)
			{
				return a.begin_time - b.begin_time;
			}
		});
    }
    
    public LableItem getLableItem(int time)
    {
    	for(LableItem item : lableItemList)
    	{
    		if(time >= item.begin_time && time <= item.end_time)
    		{
    			return item;
    		}
    		
            if(time < item.begin_time)
            {
            	return null;
            }
    	}
    	return null;
    }
    
}

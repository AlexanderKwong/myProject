package xdr.locallex;

public abstract class AStatDo
{
    protected long oneCount = 0;
    protected long oneCountMax = 100000;
	
    public AStatDo()
    {
   
    }
    
    public int stat(Object tsam)
    {
    	oneCount++;
    	return statSub(tsam);
    }  
    public abstract int statSub(Object tsam);
    
    public int outDealingResult()
    {
    	if(oneCountMax < oneCount)
    	{
    		oneCount = 0;
    		return outDealingResultSub();
    	}
    	return 0;
    }
    public abstract int outDealingResultSub();
    
    public int outFinalReuslt()
    {
		oneCount = 0;
		return outFinalReusltSub();
    }
    public abstract int outFinalReusltSub();
	
}

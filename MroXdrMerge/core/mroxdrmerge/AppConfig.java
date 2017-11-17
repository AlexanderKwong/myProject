package mroxdrmerge;

import jan.util.IConfigure;

public class AppConfig implements IConfigure
{
	private IConfigure conf;

	public AppConfig(IConfigure conf)
	{
		this.conf = conf;

		getSSHHost();
		getSSHPort();
		getSSHUser();
		getSSHPwd();
		getHadoopHost();
		getHadoopHdfsPort();
		getHadoopUser();
		getHadoopPwd();
		getMroXdrMergePath();
		getLteCellConfigPath();
		getGsmCellConfigPath();
		getTDCellConfigPath();
		getMroDataPath();
		getMreDataPath();
		getUuPath();

		// 翰信二表关联配置
		getXdrDataPath();
		getPigBinPath();
		getOrigXdrPath();
		getOrigLocationPath();
	}

	@Override
	public boolean loadConfigure()
	{
		return conf.loadConfigure();
	}

	@Override
	public boolean saveConfigure()
	{
		return conf.saveConfigure();
	}

	@Override
	public Object getValue(String name)
	{
		return conf.getValue(name);
	}

	@Override
	public Object getValue(String name, Object defaultValue)
	{
		return conf.getValue(name, defaultValue);
	}

	@Override
	public boolean setValue(String name, Object value)
	{
		return conf.setValue(name, value);
	}

	// roundSize
	public String getRoundSize()
	{
		return conf.getValue("roundSize", "40").toString();
	}

	// cellBuildPath
	public String getCellBuildPath()
	{
		return conf.getValue("cellBuildPath", "/mt_wlyh/Data/config/cellBuild").toString();
	}

	// cellBuildWifiPath
	public String getCellBuildWifiPath()
	{
		return conf.getValue("cellBuildWifiPath", "/mt_wlyh/Data/config/cellBuildWifi").toString();
	}

	// reduceNmu
	public String getreduceNum()
	{
		return conf.getValue("reduceNmu", "5").toString();
	}

	// range
	public String getRange()
	{
		return conf.getValue("range", "6").toString();
	}

	// CellNum
	public String getCellNum()
	{
		return conf.getValue("CellNum", "1").toString();
	}

	// ilongitudRrandomWidth
	public String getilongitudRrandomWidth()
	{
		return conf.getValue("ilongitudRrandomWidth", "0.0002").toString();
	}

	// ilatitudeRrandomWidth
	public String getilatitudeRrandomWidth()
	{
		return conf.getValue("ilatitudeRrandomWidth", "0.00018").toString();
	}

	// pianyiNum
	public String getPianyiNum()
	{
		return conf.getValue("pianyiNum", "1").toString();
	}

	// percent
	public String getPercent()
	{
		return conf.getValue("percent", "0.8").toString();
	}

	// figureFixdFlag
	public String getFigureFixdFlag()
	{
		return conf.getValue("figureFixFlag", "0").toString();
	}

	// reduce vcore
	public String getReduceVcore()
	{
		return conf.getValue("reduceVcore", "1").toString();
	}

	// lzoPath
	public String getLzoPath()
	{
		return conf.getValue("lzoPath", "/usr/local/lzo/lib").toString();
	}

	// mapMemory
	public String getMapMemory()
	{
		return conf.getValue("mapMemory", "4096").toString();
	}

	public String getSuYanId()
	{
		return conf.getValue("SuYanId", "").toString();
	}

	public String getSuYanQueue()
	{
		return conf.getValue("SunYanQueue", "").toString();
	}

	public String getSunYanKey()
	{
		return conf.getValue("SunYanKey", "").toString();
	}

	public String getSuYanUser()
	{
		return conf.getValue("SuYanUser", "").toString();
	}

	// reduceMemory
	public String getReduceMemory()
	{
		return conf.getValue("reduceMemory", "8192").toString();
	}

	// mapMemory
	public String getMapVcore()
	{
		return conf.getValue("mapVcore", "1").toString();
	}

	// limitSampleNum
	public String getLimitSampleNum()
	{
		return conf.getValue("limitSampleNum", "100").toString();
	}

	// size
	public String getSize()
	{
		return conf.getValue("size", "10").toString();
	}

	// cellgridSrcPath
	public String getCellgridPath()
	{
		return conf.getValue("cellgridSrcPath", "NULL").toString();
	}

	// adjustedSrcPath
	public String getAdjustedSrcPath()
	{
		return conf.getValue("adjustedSrcPath", "NULL").toString();
	}

	// srcFigurePath
	public String getSrcFigurePath()
	{
		return conf.getValue("srcFigurePath", "NULL").toString();
	}

	// eciConfigPath
	public String getEciConfigPath()
	{
		return conf.getValue("eciConfigPath", "NULL").toString();
	}

	// SSHHost
	public String getSSHHost()
	{
		return conf.getValue("SSHHost", "10.139.6.169").toString();
	}

	public boolean setSSHHost(String value)
	{
		return conf.setValue("SSHHost", value);
	}

	// SSHPort
	public int getSSHPort()
	{
		return Integer.parseInt(conf.getValue("SSHPort", 22).toString());
	}

	public boolean setSSHPort(int value)
	{
		return conf.setValue("SSHPort", value);
	}

	// SSHUser
	public String getSSHUser()
	{
		return conf.getValue("SSHUser", "root").toString();
	}

	public boolean setSSHUser(String value)
	{
		return conf.setValue("SSHUser", value);
	}

	// SSHPwd
	public String getSSHPwd()
	{
		return conf.getValue("SSHPwd", "").toString();
	}

	public boolean setSSHPwd(String value)
	{
		return conf.setValue("SSHPwd", value);
	}

	// HadoopHost
	public String getHadoopHost()
	{
		return conf.getValue("HadoopHost", "").toString();
	}

	public boolean setHadoopHost(String HadoopHost)
	{
		return conf.setValue("HadoopHost", HadoopHost);
	}

	// HadoopHdfsPort
	public int getHadoopHdfsPort()
	{
		return Integer.parseInt(conf.getValue("HadoopHdfsPort", 9000).toString());
	}

	public boolean setHadoopHdfsPort(int value)
	{
		return conf.setValue("HadoopHdfsPort", value);
	}

	// HadoopUser
	public String getHadoopUser()
	{
		return conf.getValue("HadoopUser", "root").toString();
	}

	public boolean setHadoopUser(String HadoopUser)
	{
		return conf.setValue("HadoopUser", HadoopUser);
	}

	// HadoopPwd
	public String getHadoopPwd()
	{
		return conf.getValue("HadoopPwd", "").toString();
	}

	public boolean setHadoopPwd(String HadoopPwd)
	{
		return conf.setValue("HadoopPwd", HadoopPwd);
	}

	// MaprPath
	public String getMroXdrMergePath()
	{
		return conf.getValue("MroXdrMergePath", "/mapr/").toString();
	}

	public boolean setMroXdrMergePath(String value)
	{
		return conf.setValue("MroXdrMergePath", value);
	}

	public String getInDoorSize()
	{
		return conf.getValue("indoorSize", "10").toString();
	}

	public String getOutDoorSize()
	{
		return conf.getValue("outDoorSize", "20").toString();
	}

	public String getRoundSizeIn()
	{
		return conf.getValue("roundSizeIn", "20").toString();
	}

	public String getRoundSizeOut()
	{
		return conf.getValue("roundSizeOut", "40").toString();
	}

	// LteCellConfigPath
	public String getLteCellConfigPath()
	{
		return conf.getValue("LteCellConfigPath", "/mapr/").toString();
	}

	// LteCellConfigPath
	public String getFilterCellConfigPath()
	{
		return conf.getValue("FilterCellConfigPath", "/mapr/").toString();
	}

	public String getRailCellConf()
	{
		return conf.getValue("RailCellConf", "").toString();
	}

	public String getRailStation()
	{
		return conf.getValue("RailRailStation", "").toString();
	}

	public String getRailRRU()
	{
		return conf.getValue("RailRRU", "").toString();
	}

	public String getRailConf()
	{
		return conf.getValue("RailConf", "").toString();
	}

	// ImeiConfigPath
	public String getImeiConfigPath()
	{
		return conf.getValue("imeiCapbilityPath", "/mt_wlyh/Data/config/imeiCapbilityTable.txt").toString();
	}

	// specialuser
	public String getSpecialUserPath()
	{
		return conf.getValue("specialUserList", "").toString();
	}

	public boolean setLteCellConfigPath(String value)
	{
		return conf.setValue("LteCellConfigPath", value);
	}

	// GsmCellConfigPath
	public String getGsmCellConfigPath()
	{
		return conf.getValue("GsmCellConfigPath", "/mapr/").toString();
	}

	public boolean setGsmCellConfigPath(String value)
	{
		return conf.setValue("GsmCellConfigPath", value);
	}

	// TDCellConfigPath
	public String getTDCellConfigPath()
	{
		return conf.getValue("TDCellConfigPath", "/mapr/").toString();
	}

	public boolean setTDCellConfigPath(String value)
	{
		return conf.setValue("TDCellConfigPath", value);
	}

	// MroDataPath
	public String getMroDataPath()
	{
		return conf.getValue("MroDataPath", "/mapr/").toString();
	}

	public boolean setMroDataPath(String value)
	{
		return conf.setValue("MroDataPath", value);
	}

	// MTMroDataPath
	public String getMTMroDataPath()
	{
		return conf.getValue("MTMroDataPath", "/mapr/").toString();
	}

	public boolean setMTMroDataPath(String value)
	{
		return conf.setValue("MTMroDataPath", value);
	}

	// path_ImsiCellLocPath
	public String getPath_ImsiCellLocPath()
	{
		return conf.getValue("path_ImsiCellLocPath", "/mt_wlyh/Data/config/ImsiResidentCell").toString();
	}

	// MreDataPath
	public String getMdtLogDataPath()
	{
		return conf.getValue("MdtLogDataPath", "/mapr/").toString();
	}

	// MreDataPath
	public String getMdtImmDataPath()
	{
		return conf.getValue("MdtImmDataPath", "/mapr/").toString();
	}

	// MreDataPath
	public String getMdtRlfDataPath()
	{
		return conf.getValue("MdtRlfDataPath", "/mapr/").toString();
	}

	// MreDataPath
	public String getMreDataPath()
	{
		return conf.getValue("MreDataPath", "/mapr/").toString();
	}

	public boolean setMreDataPath(String value)
	{
		return conf.setValue("MreDataPath", value);
	}

	// LocWFDataPath
	public String getLocWFDataPath()
	{
		return conf.getValue("LocWFDataPath", "/mt_wlyh/Data/loc_wf").toString();
	}

	public boolean setLocWFDataPath(String value)
	{
		return conf.setValue("LocWFDataPath", value);
	}

	//////////////////////////////////// 翰信二表合一
	//////////////////////////////////// ////////////////////////////////////////////////////////////
	// XdrDataPath
	public String getXdrDataPath()
	{
		return conf.getValue("XdrDataPath", "/result/test").toString();
	}

	public boolean setXdrDataPath(String value)
	{
		return conf.setValue("XdrDataPath", value);
	}

	// PigBinPath
	public String getPigBinPath()
	{
		return conf.getValue("PigBinPath", "/opt/pig-0.15.0/bin").toString();
	}

	public boolean setPigBinPath(String value)
	{
		return conf.setValue("PigBinPath", value);
	}

	// OrigXdrPath
	public String getOrigXdrPath()
	{
		return conf.getValue("OrigXdrPath", "/flume/xdr").toString();
	}

	public boolean setOrigXdrPath(String value)
	{
		return conf.setValue("OrigXdrPath", value);
	}

	// OrigLocationPath
	public String getOrigLocationPath()
	{
		return conf.getValue("OrigLocationPath", "/flume/location").toString();
	}

	public boolean setOrigLocationPath(String value)
	{
		return conf.setValue("OrigLocationPath", value);
	}

	/*
	 * public String getFsUri() { return "hdfs://" + getHadoopHost() + ":" +
	 * getHadoopHdfsPort(); }
	 */

	// 2017-1-10 location
	// config///////////////////////////////////////////////////
	public String getFigureConfigPath()
	{
		return conf.getValue("FigureConfigPath", "/mt_wlyh/Data/config/cellfigure").toString();
	}

	public boolean setFigureConfigPath(String value)
	{
		return conf.setValue("FigureConfigPath", value);
	}

	public String getSimuLocConfigPath()
	{
		return conf.getValue("SimuLocConfigPath", "/mt_wlyh/Data/config/simuloc").toString();
	}

	public boolean setSimuLocConfigPath(String value)
	{
		return conf.setValue("SimuLocConfigPath", value);
	}

	// 2017-5-6 location
	public String getSimuInSize()
	{
		return conf.getValue("SimuInSize", "5").toString();
	}

	public boolean setSimuInSize(String value)
	{
		return conf.setValue("SimuInSize", value);
	}

	public String getSimuOutSize()
	{
		return conf.getValue("SimuOutSize", "10").toString();
	}

	public boolean setSimuOutSize(String value)
	{
		return conf.setValue("SimuOutSize", value);
	}

	public String getSimuOrgInSize()
	{
		return conf.getValue("SimuOrgInSize", "5").toString();
	}

	public boolean setSimuOrgInSize(String value)
	{
		return conf.setValue("SimuOrgInSize", value);
	}

	public String getSimuOrgOutSize()
	{
		return conf.getValue("SimuOrgOutSize", "10").toString();
	}

	public boolean setSimuOrgOutSize(String value)
	{
		return conf.setValue("SimuOrgOutSize", value);
	}

	public String getSimuInAtte()
	{
		return conf.getValue("SimuInAtte", "0").toString();
	}

	public boolean setSimuInAtte(String value)
	{
		return conf.setValue("SimuInAtte", value);
	}

	// 几种xdr数据的配置
	public String getHttpPath()
	{
		return conf.getValue("httpPath", "/seq/news1u_http_cut/nonono/20%1$s").toString();
	}

	public String getMwPath()
	{
		return conf.getValue("mwPath", "/seq/Mw/nonono/20%1$s").toString();
	}

	public String getSvPath()
	{
		return conf.getValue("svPath", "/seq/Sv/nonono/20%1$s").toString();
	}

	public String getRxPath()
	{
		return conf.getValue("rxPath", "/seq/Rx/nonono/20%1$s").toString();
	}

	public String getMosPath()
	{
		return conf.getValue("mosPath", "%1$s/NoSatisUser/nonono/data_%2$s").toString();
	}

	public String getDhwjtPath()
	{
		return conf.getValue("dhwjtPath", "%1$s/NoSatisUser/nonono/data_%2$s/WJtdh_%2$s").toString();
	}

	public String getRtpPath()
	{
		return conf.getValue("rtpPath", "10").toString();
	}

	public String geMgPath()
	{
		return conf.getValue("mgPath", "10").toString();
	}

	public String getMmePath()
	{
		return conf.getValue("mmePath", "nonononononono10").toString();
	}

	public String getImsMoPath()
	{
		return conf.getValue("imsMoPath", "nonononononono10").toString();
	}

	public String getImsMtPath()
	{
		
		return conf.getValue("imsMtPath", "nonononononono10").toString();
	}
 
	public String getQuaLityPath()
	{
		
		return conf.getValue("quaLityPath", "nonononononono10").toString();
	}
	public String getUuPath()
	{		
		return conf.getValue("UuPath", "nonononononono10").toString();
	}
	
	public boolean setUuPath(String value)
	{
		return conf.setValue("MTMroDataPath", value);
	}
	
	
	public String getOutXdrData()
	{
		return conf.getValue("outXdrData", "false").toString();
	}

	public String getToEventData()
	{
		return conf.getValue("toEventData", "false").toString();
	}

	public String getIfOutSuccussData()
	{
		return conf.getValue("outSuccessData", "false").toString();
	}
	public String getIfOutFailData()
	{
		return conf.getValue("outFailData", "false").toString();
	}
	
	
	/**
	 * 每个map处理多少数据
	 * 
	 * @return
	 */
	public String getDealSizeMap()
	{
		return conf.getValue("dealSizeMap_M", "1024").toString();
	}

	/**
	 * 每个reduce处理多少数据
	 * 
	 * @return
	 */
	public String getDealSizeReduce()
	{
		return conf.getValue("dealSizeReduce_M", "1024").toString();
	}

	/**
	 * reduce 处理时机
	 * 
	 * @return
	 */
	public String getSlowReduce()
	{
		return conf.getValue("slowReduce", "1").toString();
	}

	/**
	 * map允许失败百分比
	 * 
	 * @return
	 */
	public String getFailPrecentMap()
	{
		return conf.getValue("failPrecentMap", "0").toString();
	}

	public String getFailPrecentReduce()
	{
		return conf.getValue("failPrecentReduce", "0").toString();
	}


}

package imsiResident.locfill.broadband;

import jan.util.GisFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import cellconfig.CellConfig;
import cellconfig.LteCellInfo;
import model.BroadBand;
import model.ImsiResident;

public class ImsiResidentBroadbandDeal
{
	
	private ResultHandler resultHandler;
	
	private CellConfig cellConfig;
	
	private Set<String> cell_build;
	
	public final static int DISTANCE_THRESHOLD = 200;
	
	public ImsiResidentBroadbandDeal(){
		this(null);
	}
	
	public ImsiResidentBroadbandDeal(ResultHandler resultHandler){
		this.resultHandler = resultHandler;
		init();
	}
	
	private void init(){
		//加载小区楼宇
		cell_build = new HashSet<>();
		//加载工参
		cellConfig = CellConfig.GetInstance();
		//TODO 
//		cellConfig.loadLteCell(new Configuration());
	}
	/**
	 * 同一手机号的数据
	 * @param userInfo 常驻用户数据
	 * @param broadBandInfo 家庭宽带数据
	 */
	public void deal(List<ImsiResident> userInfo, List<BroadBand> broadBandInfo){

		for(ImsiResident user : userInfo){//PS. 实现LEFTJOIN， 此处循环不能反过来嵌套
			for(BroadBand broadBand : broadBandInfo){
				if(seekInCellBuild(user.getEci(), broadBand.getBuildId())
					|| seekInCellConf(user.getEci(), user.getTime(), broadBand.getBuildLongtitude(), broadBand.getBuildLattitude())){
					fillLoc(user, broadBand);
					break;
				}
			}
			if(resultHandler != null)
				resultHandler.put(user.toString());
		}	
		//TODO 吐出用户
		if(resultHandler != null)
			resultHandler.flush();
	}
	
	private boolean seekInCellBuild(long eci, int buildId){
		return cell_build.contains(eci + "_" + buildId);
	}
	
	private void fillLoc(ImsiResident user, BroadBand broadBand){
		user.setLongtitude(broadBand.getBuildLongtitude());
		user.setLattitude(broadBand.getBuildLattitude());
		user.setBuildId(broadBand.getBuildId());
		user.setBuildLongtitude(broadBand.getBuildLongtitude());
		user.setBuildLattitude(broadBand.getBuildLattitude());
	}
	
	private boolean seekInCellConf(long eci, int time, int buildLongtitude, int buildLattitude){
		LteCellInfo cellInfo = cellConfig.getLteCell(eci);
		
		if(cellInfo != null){
			if(time >= 23 && time <= 6 && GisFunction.GetDistance(buildLongtitude, buildLattitude, cellInfo.ilongitude, cellInfo.ilatitude) < DISTANCE_THRESHOLD){
				return true;
			}
		}
		return false;
	}
	
	
}

package net.floodlightcontroller.sflowcollector;


import java.util.Map;


import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.types.NodePortTuple;


public interface ISflowCollectionService extends IFloodlightService {
	
	public static final String enabledPropStr = "net.floodlightcontroller.sflowcollector.enabled";
	//public static final String enabledPropStr = "true";
	
	public abstract void addSflowListener(ISflowListener listener);
	
	public abstract void removeSflowListener(ISflowListener listener);
	
	public Map<NodePortTuple,InterfaceStatistics > getStatisticsMap();
	//参考带宽收集模块所得
	
}

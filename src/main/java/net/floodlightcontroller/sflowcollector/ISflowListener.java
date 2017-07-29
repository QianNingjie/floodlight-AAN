package net.floodlightcontroller.sflowcollector;

import java.io.IOException;
import java.util.Map;

public interface ISflowListener {

	public abstract void sflowCollected(Map<Integer, InterfaceStatistics> ifIndexIfStatMap) throws IOException;
}

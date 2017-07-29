package net.floodlightcontroller.sflowcollector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.FloodlightModuleLoader;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;

import net.floodlightcontroller.core.types.NodePortTuple;
import org.json.JSONException;
import org.json.JSONObject;
import org.projectfloodlight.openflow.types.OFPort;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SflowCollector implements IFloodlightModule, ISflowCollectionService {
	public static final String sflowRtUriPropStr = "net.floodlightcontroller.sflowcollector.SflowCollector.uri";
	
	public static final long DEFAULT_FIRST_DELAY = 10000L; //在启动的10秒之后执行
	public static final long DEFAULT_PERIOD = 10000L; //收集周期
	protected IOFSwitchService switchService;
	protected Map<Integer, InterfaceStatistics> ifIndexIfStatMap;  // sflow中的端口和收集的数据映射，注意这里的端口并不是交换机的端口
	protected Set<ISflowListener> sflowListeners;
	protected String sFlowRTURI; // json网址
	protected long firstDelay; //定时任务的参数1
	protected long period; // 定时任务的参数2
	protected static Logger log;
	List<String> agentIps;
	protected Map<NodePortTuple,InterfaceStatistics > swStats;//交换机端口 ----- 数据
	
	// 本模块提供的服务
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		Collection<Class<? extends IFloodlightService>> l =
				new ArrayList<Class<? extends IFloodlightService>>();
		l.add(ISflowCollectionService.class); 
		return l;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		Map<Class<? extends IFloodlightService>, IFloodlightService> m =
				new HashMap<Class<? extends IFloodlightService>, IFloodlightService>();
        // We are the class that implements the service
        m.put(ISflowCollectionService.class, this);
        return m;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l =
				new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		l.add(IOFSwitchService.class); // the service we need to depend on
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context) //初始化
			throws FloodlightModuleException {
	    switchService = context.getServiceImpl(IOFSwitchService.class);
		sflowListeners = new CopyOnWriteArraySet<ISflowListener>(); // 参考http://ifeve.com/tag/copyonwritearrayset/
		ifIndexIfStatMap = new ConcurrentHashMap<Integer, InterfaceStatistics>(); // 参考  http://www.iteye.com/topic/1103980	
		swStats = new ConcurrentHashMap<NodePortTuple,InterfaceStatistics >();
		// 上述三个容器用Concurrent都是为了并发操作的线程安全
		log = LoggerFactory.getLogger(SflowCollector.class);
	}

	@Override
	public void startUp(FloodlightModuleContext context) {  //模块启动
		Properties prop = new Properties(); //java 配置属性文件
		InputStream is = this.getClass().getClassLoader().
                getResourceAsStream(FloodlightModuleLoader.COMPILED_CONF_FILE);//从属性文件floodlightdefault.properties读入模块配置属性
		try {
			prop.load(is);
		} catch (IOException e) {
		//.error("Could not load sFlow-RT URI configuration file", e);
			System.exit(1);
		}
		boolean enabled = Boolean.parseBoolean(prop.getProperty(ISflowCollectionService.enabledPropStr, "true"));
		if(!enabled) {
			System.err.println("SflowCollector Not Enabled.");
			return;
		}else
			log.info("SflowCollector enabled");

		sFlowRTURI = prop.getProperty(sflowRtUriPropStr);//得到sflow-rt的Uniform Resource Identifier
		if(sFlowRTURI == null || sFlowRTURI.length() == 0) {
			System.err.println("Could not load sFlow-RT URI configuration file");
			System.exit(1);
		}else{
			System.out.println(sFlowRTURI);
		}
		//设置收集任务的参数
		firstDelay = DEFAULT_FIRST_DELAY;
		period = DEFAULT_PERIOD; 
		
		agentIps = new ArrayList<String>(); //读入 sflow agent ip
		readIpFile("inputFile/agent_ip.txt",agentIps); //注意：pay attention to 文件结束，不要有多的空行之类的！！！！
		
		/*for(String agentIp : agentIps) {
			System.out.println(agentIp);
		}*/
		
		Timer t = new Timer("SflowCollectionTimer"); //定义定时任务
		t.schedule(new SflowCollectTimerTask(), firstDelay, period); //在任务启动firstDelay ms之后以period的周期进行调度
		// 参考 http://www.yiibai.com/java/util/timer_schedule_period.html		
	}
	
	private void sflowCollect(String baseUri, String agentIp) {
		
		String uri = baseUri.replace("{agent_ip}", agentIp);	
		// 根据uri得到资源     org.restlet.jar
		ClientResource resource = new ClientResource(uri); 
		//restlet  建立REST概念与Java类之间的映射
		// 例子参考  http://blog.csdn.net/is_zhoufeng/article/details/9719783
	
		System.err.println("--- uri " + uri);
		
    	Representation r = resource.get();//得到资源的表达
		
		JsonRepresentation jr = null;
		JSONObject jo = null;
		try {
			jr = new JsonRepresentation(r);
			jo = jr.getJsonObject(); //由json的表达得到json对象
			// JSON(JavaScript Object Notation) 是一种数据交换格式
		} catch (IOException e) {
			
		} catch (JSONException e) {
			
		}
		if(jo == null) {
			System.err.println("Get JSON failed.");
		}
		
		//记录对应agentIp的switch数据
		TreeMap<Integer,InterfaceStatistics> switchdata = new TreeMap<Integer,InterfaceStatistics>();			
		@SuppressWarnings("unchecked")
		Iterator<String> it = jo.keys();
		while(it.hasNext()) {
			String key = it.next();	// key 对应	eg："3.ifadminstatus"	
			String statProp = key.substring(key.indexOf(".") + 1);	// statProp 对应	ifadminstatus
			if(InterfaceStatistics.ALLJSONPROPERTIES.contains(statProp)) {
				Integer ifIndex = -1;
				try {
					ifIndex = Integer.parseInt(key.substring(0, key.indexOf("."))); //ifIndex对应3
					
				} catch(NumberFormatException e) {
					continue;
				}
				
				if (ifIndex >= 0) {
					String ifName = null; //ifName可以不要，把InterfaceStatistics相应的构造函数改成一个参数就行
					if (!ifIndexIfStatMap.containsKey(ifIndex)) {
						ifIndexIfStatMap.put(ifIndex, new InterfaceStatistics(ifIndex, ifName));
					}
					InterfaceStatistics is = ifIndexIfStatMap.get(ifIndex);	
					//从Json数据的key，从jo得到对应的value值，建立InterfaceStatistics对象	
					is.fromJsonProp(key, jo); 						
					switchdata.put(ifIndex, is);		
				}
				else {
					System.err.println("------cannot get ifIndex from sflowdata");
				}				
			}
		}
		
		/*// output test
		System.out.println("start switchdata");
		for(InterfaceStatistics is:switchdata.values()){
			System.out.println(is.getIfIndex()+is.toString());
		}	
		System.out.println("end switchdata");*/
		
		//交换机端口映射  sflow ----- floodlight	
		int num = (int)parseSting(agentIp) - 100; //agentIp 10.0.0.101  , 提取出101,这里属于自定义的处理，为了标识交换机id号，1号交换机最终num=1
		for (IOFSwitch sw : switchService.getAllSwitchMap().values()) { 		
			//System.out.println("switch  long " + sw.getId().getLong()); // 2 交换机 dpid					
			if(sw.getId().getLong()== num) // 交换机对上agentIp表示的switch号
			{
				TreeMap<Integer,OFPort> ports = new TreeMap<Integer,OFPort>();	
				for (OFPort p : sw.getEnabledPortNumbers()) // 交换机端口号集合
				{
					if(p.toString()	=="local")  //排除local端口
						continue;
					ports.put(p.getPortNumber(), p);
					//System.out.println(" port "+ p.getPortNumber());	
				}
				
				ArrayList<Integer> parr = new ArrayList<Integer>();
				for(int pi : ports.keySet()){
					parr.add(pi);
				}
				
				int i = 0;
				for(InterfaceStatistics is:switchdata.values()){
					if(i==parr.size()) break;
					if(is.getifDirection().equals("unknown")) //sflow local 特征
						continue; // sflow里面的local端口号的特征是ifdirection = unknown ifspeed=1.0e08
					is.setport(parr.get(i)); //设置交换机真正的端口号，来自floodlight里得到的端口号
					NodePortTuple npt = new NodePortTuple(sw.getId(), OFPort.ofInt(parr.get(i)));  
					// OFPort.ofInt单例模式构造对象
					swStats.put(npt,is);
					i++;
				}				
				break; //对应第一个if ： 遍历到agentIp对应的交换机，且将相应数据放入swStats之后，break，可以不用再遍历后面的IOFSwitch
			}							
		}	
		
		/*// 端口映射完成测试
		System.out.println("switchdata 2");
		for(InterfaceStatistics is:switchdata.values()){
			System.out.println(is.getIfIndex()+is.toString());
		}
		
		// 所有交换机端口数据 output test
		System.out.println("-------");
		System.out.println("sFlow Collector Stats switch bandwidth Rx BitsPerSecond");
		for (NodePortTuple npt : swStats.keySet()) {
			System.out.println(npt + " ---- " + swStats.get(npt).getIfOutOctets() * 8); // 输出特定的参数
		}*/
		
	}
		
	@Override
	public void addSflowListener(ISflowListener listener) {
		sflowListeners.add(listener);
	}
	
	@Override
	public void removeSflowListener(ISflowListener listener) {
		sflowListeners.remove(listener);
	}
	
	@Override
	public Map<NodePortTuple,InterfaceStatistics > getStatisticsMap(){
		return Collections.unmodifiableMap(swStats);
	}
	
	// 读入sflow agent ip 文件
	public void readIpFile(String fileName, List<String> agentIps) {
		// 手动加ip测试					
		/*
		 * String ip = "10.0.0.100"; agentIps.add(ip);
		 * agentIps.add("10.0.0.101"); System.err.println("----- ip");
		 * for(String agentIp : agentIps) { System.err.println(agentIp); }
		 * System.err.println("file ip"); for(String agentIp : agentIps) {
		 * System.err.println(agentIp); }
		 */
		File file = new File(fileName);
		BufferedReader reader = null;
		if (file.isFile() && file.exists()) {
			try {
				reader = new BufferedReader(new FileReader(file));
				String ip = null;
				while ((ip = reader.readLine()) != null) {
					agentIps.add(ip);
				}
				reader.close();
			} catch (IOException e) {
				System.err.println("File Error!");
			}finally {
	            if (reader != null) {
	                try {
	                    reader.close();
	                } catch (IOException e1) {
	                }
	            }
		   }
		}else {
			System.err.println("can not find flie agent_ip.txt ");
		}
	}
	
	public long parseSting(String agentIp){
		// test 字符串分割
		/*
		 * String a = "127.0.0.101"; String[] as = a.split("\\."); for(String
		 * s:as){ System.out.print(s+"  "); } int num = Integer.parseInt(as[3]);
		 * System.err.println("num"+num);
		 */
		String[] as = agentIp.split("\\.");  //因为.是转义字符，所以必须用\\.表示以.分割
		//for(String s:as){ System.out.print(s+"  ");}  // output test
		int id = Integer.parseInt(as[3]);
		//System.err.println("switch id "+id); // test
		return id;
	}
	
	// 停止问题 ？？？？
	// TimerTask 可以实现多线程
	private class SflowCollectTimerTask extends TimerTask {
		@Override
		public void run() {	
	
			for(String agentIp : agentIps) {
				sflowCollect(sFlowRTURI, agentIp);		
			    
				// listener 
				try{
					for(ISflowListener sflowListener : sflowListeners) 
						sflowListener.sflowCollected(ifIndexIfStatMap) ;
				}
				catch(IOException e){
					System.out.println(e);
				}
				
			}		
			// 所有交换机端口数据 output test
			/*
			System.out.println("sFlow Collector Stats switch bandwidth Rx BitsPerSecond");
			for(NodePortTuple npt:swStats.keySet()){
				//System.out.println( npt+" ---- "+swStats.get(npt).getIfOutOctets()*8 + "  pkt/s ---- " +swStats.get(npt).getIfOutpkts());  //输出特定的参数
			    log.info("{} ---{}",npt,swStats.get(npt).getIfOutOctets()*8);
			}
			*/
		}
	}
}

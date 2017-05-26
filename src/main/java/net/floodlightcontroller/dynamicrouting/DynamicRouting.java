package net.floodlightcontroller.dynamicrouting;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import sun.misc.BASE64Encoder;

import org.projectfloodlight.openflow.protocol.OFFlowModCommand;
import org.projectfloodlight.openflow.protocol.OFFlowRemoved;
import org.projectfloodlight.openflow.protocol.OFFlowRemovedReason;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFPacketOut;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.IPv6Address;
import org.projectfloodlight.openflow.types.IpProtocol;
import org.projectfloodlight.openflow.types.MacAddress;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.OFVlanVidMatch;
import org.projectfloodlight.openflow.types.TransportPort;
import org.projectfloodlight.openflow.types.U16;
import org.projectfloodlight.openflow.types.U64;
import org.projectfloodlight.openflow.types.VlanVid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.types.NodePortTuple;
import net.floodlightcontroller.core.util.AppCookie;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.SwitchPort;
import net.floodlightcontroller.linkdiscovery.ILinkDiscovery.LDUpdate;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.packet.IPv6;
import net.floodlightcontroller.packet.TCP;
import net.floodlightcontroller.packet.UDP;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Path;
import net.floodlightcontroller.routing.PathId;
import net.floodlightcontroller.statistics.IStatisticsService;
import net.floodlightcontroller.threadpool.IThreadPoolService;
import net.floodlightcontroller.topology.ITopologyListener;
import net.floodlightcontroller.topology.ITopologyService;
import net.floodlightcontroller.util.OFMessageUtils;

public class DynamicRouting extends DynamicRoutingBase implements IFloodlightModule, ITopologyListener{
	private static final Logger log = LoggerFactory.getLogger(DynamicRouting.class);
	
	private ITopologyService topologyService;

	
	private static boolean isEnabled = false;
	private Map<PathId, List<Path>> pathcache;
	private Map<PathId, Integer> nextIndex;
	
	private boolean isEmergency = false;
	
	private static FlowRegistry flowRegistry;
	private static final String ENABLED_STR = "enable";
	private static final long FLOWSET_BITS = 52;
//    private static final long FLOWSET_MASK = ((1L << FLOWSET_BITS) - 1);
    private static final long FLOWSET_MAX = (long) (Math.pow(2, FLOWSET_BITS) - 1);
    
    private static final int FLOOR = 20;   
    private static final int CEILING = 40;
    private static final IPv4Address client = IPv4Address.of("192.168.56.11");
    private static final IPv4Address server = IPv4Address.of("192.168.56.12");
    private static final TransportPort serverPort = TransportPort.of(3000);
   
    private static class FlowRecord{
		private U64 cookie;
		private Match match;
		private OFPort srcPort;
		private OFPort dstPort;
	
		FlowRecord(U64 cookie, Match match, OFPort srcPort, OFPort dstPort){
			this.cookie = cookie;
			this.match = match;
			this.srcPort = srcPort;
			this.dstPort = dstPort;
		}
	}
    
	private static class FlowRegistry{
		private volatile Map<Path, Set<FlowRecord>> pathToFlowSet;
		private volatile Map<U64, Path> vipToPath;
		
		private volatile long flowIdGenerator = -1;
		private static volatile FlowRegistry instance;
		
		private FlowRegistry(){
			pathToFlowSet = new ConcurrentHashMap<Path, Set<FlowRecord>>();
			vipToPath = new ConcurrentHashMap<U64, Path>();
		}
		
		private static FlowRegistry getInstance() {
            if (instance == null) 
                instance = new FlowRegistry();
            return instance;
        }
		
		private synchronized long generateFlowId() {
			flowIdGenerator += 1;
	        if (flowIdGenerator == FLOWSET_MAX) {
	            flowIdGenerator = 0;
	            log.warn("Flowset IDs have exceeded capacity of {}. Flowset ID generator resetting back to 0", FLOWSET_MAX);
	        }
            log.debug("Generating flowset ID {}", flowIdGenerator);
	        return flowIdGenerator;
	    }
		 
		private void register(Path path, FlowRecord flowRecord){
			if(pathToFlowSet.containsKey(path))
				pathToFlowSet.get(path).add(flowRecord);
			else{
				Set<FlowRecord> flowSet = new HashSet<>();
				flowSet.add(flowRecord);
				pathToFlowSet.put(path, flowSet);
			}	 
		}
		
		private void registerVip(U64 cookie, Path path){
			vipToPath.put(cookie, path);
		}
		
		private void removeExpiredFlow(U64 cookie){
			Iterator<Map.Entry<Path, Set<FlowRecord>>> it = pathToFlowSet.entrySet().iterator();
			while(it.hasNext()){
				Map.Entry<Path, Set<FlowRecord>> entry = it.next();
				Iterator<FlowRecord> itt = entry.getValue().iterator();
				while(itt.hasNext()){
					FlowRecord flow = itt.next();
					if(flow.cookie.equals(cookie)){
						it.remove();
					}
				}
				if(entry.getValue().isEmpty())
					it.remove();
			}
			
			Iterator<Map.Entry<U64, Path>> itv = vipToPath.entrySet().iterator();
			while(itv.hasNext())
				if(itv.next().getKey().equals(cookie))
					itv.remove();
			
			/*
			System.out.println();
			if(!vipToPath.isEmpty())
				for(U64 coki : vipToPath.keySet())
					System.out.println(coki);
			else
				System.out.println("no vip flow");
		
			System.out.println();
			if(!pathToFlowSet.isEmpty())
				for(Set<FlowRecord> flowSet : pathToFlowSet.values()){
					for(FlowRecord flow : flowSet)
						System.out.println(flow.cookie);
				}
			else
				System.out.println("no normal flow");
			*/
		}
		
		private boolean hasVip(){
			return !vipToPath.isEmpty();
		}
		
		private Iterator<Path> vipPaths(){
			return vipToPath.values().iterator();
		}
		 
		private void clearFlowSet(Path path){
			pathToFlowSet.remove(path);
		}
		
		private Set<FlowRecord> getFlow(Path path){
			return pathToFlowSet.get(path);
		}
	}
	
	@Override
	public void topologyChanged(List<LDUpdate> linkUpdates){
		/*
		for(PathId pathId : pathcache.keySet()){
			List<Path> paths = routingEngineService.getPathsFast(pathId.getSrc(), pathId.getDst());
			StringBuffer sb = new StringBuffer();
			sb.append(pathId.toString());
			for(Path path : paths){
				sb.append(path.getPathIndex());
				sb.append(" ");
			}
			sb.append("\n");
			log.info(sb.toString());

		}
		*/

	}
	
	private void increaseIndex(PathId pathId){
		int tmp =  (nextIndex.get(pathId) + 1) % pathcache.get(pathId).size();
		nextIndex.put(pathId, tmp);
	}
	
	private Path getAPath(PathId pathId){
		if(!pathcache.containsKey(pathId)){
			List<Path> paths = routingEngineService.getPathsFast(pathId.getSrc(), pathId.getDst());
			pathcache.put(pathId, paths);
			nextIndex.put(pathId, 0);
		}
		
		if(isEmergency && flowRegistry.hasVip()){
			Iterator<Path> it = flowRegistry.vipPaths();
			while(it.hasNext()){
				Path path = it.next();
				if(path.getId() == pathId)
					if(path.getPathIndex() == nextIndex.get(pathId))
						increaseIndex(pathId);	
			}
		}

		Path ret = pathcache.get(pathId).get(nextIndex.get(pathId));
		increaseIndex(pathId);
		return ret;
	}
	
	private void reroute(Path path){
		Set<FlowRecord> flowSet = flowRegistry.getFlow(path);
		if(flowSet == null) {
			log.info("no conlict, just waiting");
			return;
		}

		PathId pathId = path.getId();
		System.out.println();
		log.info("vip monopoly " + pathId.getSrc() + " -> " + pathId.getDst() + " : " + path.getPathIndex());

		Iterator<FlowRecord> it = flowSet.iterator();
		while(it.hasNext()){
			FlowRecord flow = it.next();
			Path noAp = getAPath(pathId);
			log.info("poor guy : {}， ", flow.match);
			log.info("changed to {}", noAp.getPathIndex());
			List<NodePortTuple> nptList = new ArrayList<>(noAp.getPath());
			NodePortTuple npt = new NodePortTuple(pathId.getSrc(), flow.srcPort);
			nptList.add(0, npt);
			npt = new NodePortTuple(pathId.getDst(), flow.dstPort);
			nptList.add(npt);
			Path newPath = new Path(pathId, nptList);
			newPath.setPathIndex(noAp.getPathIndex());;
			pushRoute(newPath, flow.match, null, null, flow.cookie, true, OFFlowModCommand.ADD);
//			log.info("new path pushed");
			removeFlow(path, flow.cookie);
//			log.info("old flow {} removed", flow.cookie);
			flowRegistry.register(noAp, flow);
		}

		flowRegistry.clearFlowSet(path);
	}
	
	protected void doForwardFlow(IOFSwitch sw, OFPacketIn pi, FloodlightContext cntx) {
        OFPort srcPort = OFMessageUtils.getInPort(pi);
        DatapathId srcSw = sw.getId();
        IDevice dstDevice = IDeviceService.fcStore.get(cntx, IDeviceService.CONTEXT_DST_DEVICE);
        IDevice srcDevice = IDeviceService.fcStore.get(cntx, IDeviceService.CONTEXT_SRC_DEVICE);

        if (dstDevice == null) {
            log.debug("Destination device unknown. Flooding packet");
            doFlood(sw, pi,cntx);
            return;
        }

        if (srcDevice == null) {
            log.error("No device entry found for source device. Is the device manager running? If so, report bug.");
            return;
        }

        /* Some physical switches partially support or do not support ARP flows */
        if (FLOOD_ALL_ARP_PACKETS && 
                IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD).getEtherType() 
                == EthType.ARP) {
            log.debug("ARP flows disabled in Forwarding. Flooding ARP packet");
            doFlood(sw, pi, cntx);
            return;
        }

        /* This packet-in is from a switch in the path before its flow was installed along the path */
        if (!topologyService.isEdge(srcSw, srcPort)) {  
            log.debug("Packet destination is known, but packet was not received on an edge port (rx on {}/{}). Flooding packet", srcSw, srcPort);
            doFlood(sw, pi, cntx);
            return; 
        }   

        /* 
         * Search for the true attachment point. The true AP is
         * not an endpoint of a link. It is a switch port w/o an
         * associated link. Note this does not necessarily hold
         * true for devices that 'live' between OpenFlow islands.
         * 
         * TODO Account for the case where a device is actually
         * attached between islands (possibly on a non-OF switch
         * in between two OpenFlow switches).
         */
        SwitchPort dstAp = null;
        for (SwitchPort ap : dstDevice.getAttachmentPoints()) {
            if (topologyService.isEdge(ap.getNodeId(), ap.getPortId())) {
                dstAp = ap;
                break;
            }
        }	

        /* 
         * This should only happen (perhaps) when the controller is
         * actively learning a new topology and hasn't discovered
         * all links yet, or a switch was in standalone mode and the
         * packet in question was captured in flight on the dst point
         * of a link.
         */
        if (dstAp == null) {
            log.debug("Could not locate edge attachment point for destination device {}. Flooding packet");
            doFlood(sw, pi, cntx);
            return; 
        }

        /* Validate that the source and destination are not on the same switch port */
        if (sw.getId().equals(dstAp.getNodeId()) && srcPort.equals(dstAp.getPortId())) {
        	if(srcSw.equals(DatapathId.of("00:00:00:00:00:00:00:01")) && srcPort.equals(OFPort.of(7)))
        		return;
        	log.info(srcSw.toString() + " vs " + DatapathId.of("00:00:00:00:00:00:00:02").toString() + "  "
        			+ srcPort.toString() + " vs " + OFPort.of(7).toString() );
            log.info("Both source and destination are on the same switch/port {}/{}. Dropping packet", sw.toString(), srcPort);
            return;
        }		
        
        //record[0]:该流是否需要记录，只有tcp和udp流要被记录
        //record[1]:该流是否是vip流，是的话要生成记录它走的路
        Boolean[] flag = new Boolean[2]; 
        flag[0] = false;
        flag[1] = false;
       
        Match m = createMatchFromPacket(sw, srcPort, pi, cntx, flag);
        
//        System.out.println();
//        log.info("{}", m);
        
        long user_fields = flowRegistry.generateFlowId();
        U64 cookie = AppCookie.makeCookie(DYNAMIC_ROUTING_APP_ID, user_fields);
      
        PathId pathId = new PathId(srcSw, dstAp.getNodeId());
        Path noAp = null;
        List<NodePortTuple> nptList;
        if(!srcSw.equals(dstAp.getNodeId())){
			noAp = getAPath(pathId);
			nptList = new ArrayList<>(noAp.getPath());
		}else
			nptList = new ArrayList<>();
        
        NodePortTuple npt = new NodePortTuple(srcSw, srcPort);
		nptList.add(0, npt);
		npt = new NodePortTuple(dstAp.getNodeId(), dstAp.getPortId());
		nptList.add(npt);
		Path path = new Path(pathId, nptList);
		if(noAp != null)
			path.setPathIndex(noAp.getPathIndex());


        if (!path.getPath().isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("pushRoute inPort={} route={} " +"destination={}:{}",
                        new Object[] { srcPort, path,dstAp.getNodeId(),  dstAp.getPortId()});
                log.debug("Creating flow rules on the route, match rule: {}", m);
            }

            pushRoute(path, m, pi, sw.getId(), cookie, flag[0], OFFlowModCommand.ADD);	
  
            
            //This is done after we push the path as it is blocking.
            if(noAp == null)
            	return;
            
            if(flag[1]){
            	flowRegistry.registerVip(cookie, noAp);
            	System.out.println();
            	log.info("------vip flow--------");
            	log.info(pathId.getSrc().getLong() + " -> " + pathId.getDst().getLong() 
            			+ ", choosed path : " + path.getPathIndex() + ", cookie：" + cookie);
            	log.info("match : {}", m);
                
            }else if(flag[0]){
            	FlowRecord flowRecord = new FlowRecord(cookie, m, srcPort, dstAp.getPortId());
            	flowRegistry.register(noAp, flowRecord);
            	System.out.println();
            	log.info("------flow registry--------");
            	log.info(pathId.getSrc().getLong() + " -> " + pathId.getDst().getLong()
            			+ ", choosed path : " + path.getPathIndex() + ", cookie：" + cookie);
            	log.info("match : {}", m);
            }
           
        } 
    }
	

    protected Match createMatchFromPacket(IOFSwitch sw, OFPort inPort, OFPacketIn pi, FloodlightContext cntx, Boolean[] flag) {
        // The packet in match will only contain the port number.
        // We need to add in specifics for the hosts we're routing between.
        Ethernet eth = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
        VlanVid vlan = null;      
        if (pi.getVersion().compareTo(OFVersion.OF_11) > 0 && /* 1.0 and 1.1 do not have a match */
                pi.getMatch().get(MatchField.VLAN_VID) != null) { 
            vlan = pi.getMatch().get(MatchField.VLAN_VID).getVlanVid(); /* VLAN may have been popped by switch */
        }
        if (vlan == null) {
            vlan = VlanVid.ofVlan(eth.getVlanID()); /* VLAN might still be in packet */
        }
        
        MacAddress srcMac = eth.getSourceMACAddress();
        MacAddress dstMac = eth.getDestinationMACAddress();

        Match.Builder mb = sw.getOFFactory().buildMatch();
        if (FLOWMOD_DEFAULT_MATCH_IN_PORT) {
            mb.setExact(MatchField.IN_PORT, inPort);
        }

        if (FLOWMOD_DEFAULT_MATCH_MAC) {
            if (FLOWMOD_DEFAULT_MATCH_MAC_SRC) {
                mb.setExact(MatchField.ETH_SRC, srcMac);
            }
            if (FLOWMOD_DEFAULT_MATCH_MAC_DST) {
                mb.setExact(MatchField.ETH_DST, dstMac);
            }
        }

        if (FLOWMOD_DEFAULT_MATCH_VLAN) {
            if (!vlan.equals(VlanVid.ZERO)) {
                mb.setExact(MatchField.VLAN_VID, OFVlanVidMatch.ofVlanVid(vlan));
            }
        }

        // TODO Detect switch type and match to create hardware-implemented flow
        if (eth.getEtherType() == EthType.IPv4) { /* shallow check for equality is okay for EthType */
            IPv4 ip = (IPv4) eth.getPayload();
            IPv4Address srcIp = ip.getSourceAddress();
            IPv4Address dstIp = ip.getDestinationAddress();

            if (FLOWMOD_DEFAULT_MATCH_IP) {
                mb.setExact(MatchField.ETH_TYPE, EthType.IPv4);
                if (FLOWMOD_DEFAULT_MATCH_IP_SRC) {
                    mb.setExact(MatchField.IPV4_SRC, srcIp);
                }
                if (FLOWMOD_DEFAULT_MATCH_IP_DST) {
                    mb.setExact(MatchField.IPV4_DST, dstIp);
                }
            }

            if (FLOWMOD_DEFAULT_MATCH_TRANSPORT) {
                /*
                 * Take care of the ethertype if not included earlier,
                 * since it's a prerequisite for transport ports.
                 */
            	
                if (!FLOWMOD_DEFAULT_MATCH_IP) {
                    mb.setExact(MatchField.ETH_TYPE, EthType.IPv4);
                }
               
                if (ip.getProtocol().equals(IpProtocol.TCP)) {
                	if((srcIp.equals(server) && dstIp.equals(client)) ||
                		(srcIp.equals(client) && dstIp.equals(server)) )
                		flag[0] = true;
                    TCP tcp = (TCP) ip.getPayload();
                    mb.setExact(MatchField.IP_PROTO, IpProtocol.TCP);
                    if(srcIp.equals(server) && tcp.getSourcePort().equals(serverPort)){
                    	flag[1] =true;
                    	mb.setExact(MatchField.TCP_SRC, tcp.getSourcePort());
                    }else if(dstIp.equals(server) && tcp.getDestinationPort().equals(serverPort)){
                    	flag[1] =true;
                    	mb.setExact(MatchField.TCP_DST, tcp.getDestinationPort());
                    }else{
                    	if (FLOWMOD_DEFAULT_MATCH_TRANSPORT_SRC) 
                    		mb.setExact(MatchField.TCP_SRC, tcp.getSourcePort());
                    	if (FLOWMOD_DEFAULT_MATCH_TRANSPORT_DST) 
                    		mb.setExact(MatchField.TCP_DST, tcp.getDestinationPort());
                    }
                    /*
                    if(
                    sw.getSwitchDescription().getHardwareDescription().toLowerCase().contains("open vswitch") && (
                    Integer.parseInt(sw.getSwitchDescription().getSoftwareDescription().toLowerCase().split("\\.")[0]) > 2  || (
                    Integer.parseInt(sw.getSwitchDescription().getSoftwareDescription().toLowerCase().split("\\.")[0]) == 2 &&
                    Integer.parseInt(sw.getSwitchDescription().getSoftwareDescription().toLowerCase().split("\\.")[1]) >= 1 ))
                    ){
	                    if(FLOWMOD_DEFAULT_MATCH_TCP_FLAG){
	                        mb.setExact(MatchField.OVS_TCP_FLAGS, U16.of(tcp.getFlags()));
	                    }
                    }
                    */
                } else if (ip.getProtocol().equals(IpProtocol.UDP)) {
                	flag[0] = true;
                    UDP udp = (UDP) ip.getPayload();
                    mb.setExact(MatchField.IP_PROTO, IpProtocol.UDP);
                    if (FLOWMOD_DEFAULT_MATCH_TRANSPORT_SRC) {
                        mb.setExact(MatchField.UDP_SRC, udp.getSourcePort());
                    }
                    if (FLOWMOD_DEFAULT_MATCH_TRANSPORT_DST) {
                        mb.setExact(MatchField.UDP_DST, udp.getDestinationPort());
                    }
                }
            }
        } else if (eth.getEtherType() == EthType.ARP) { /* shallow check for equality is okay for EthType */
            mb.setExact(MatchField.ETH_TYPE, EthType.ARP);
        } else if (eth.getEtherType() == EthType.IPv6) {
            IPv6 ip = (IPv6) eth.getPayload();
            IPv6Address srcIp = ip.getSourceAddress();
            IPv6Address dstIp = ip.getDestinationAddress();

            if (FLOWMOD_DEFAULT_MATCH_IP) {
                mb.setExact(MatchField.ETH_TYPE, EthType.IPv6);
                if (FLOWMOD_DEFAULT_MATCH_IP_SRC) {
                    mb.setExact(MatchField.IPV6_SRC, srcIp);
                }
                if (FLOWMOD_DEFAULT_MATCH_IP_DST) {
                    mb.setExact(MatchField.IPV6_DST, dstIp);
                }
            }

            if (FLOWMOD_DEFAULT_MATCH_TRANSPORT) {
                /*
                 * Take care of the ethertype if not included earlier,
                 * since it's a prerequisite for transport ports.
                 */
                if (!FLOWMOD_DEFAULT_MATCH_IP) {
                    mb.setExact(MatchField.ETH_TYPE, EthType.IPv6);
                }

                if (ip.getNextHeader().equals(IpProtocol.TCP)) {
                    TCP tcp = (TCP) ip.getPayload();
                    mb.setExact(MatchField.IP_PROTO, IpProtocol.TCP);
                    if (FLOWMOD_DEFAULT_MATCH_TRANSPORT_SRC) {
                        mb.setExact(MatchField.TCP_SRC, tcp.getSourcePort());
                    }
                    if (FLOWMOD_DEFAULT_MATCH_TRANSPORT_DST) {
                        mb.setExact(MatchField.TCP_DST, tcp.getDestinationPort());
                    }
                    if(
                    sw.getSwitchDescription().getHardwareDescription().toLowerCase().contains("open vswitch") && (
                    Integer.parseInt(sw.getSwitchDescription().getSoftwareDescription().toLowerCase().split("\\.")[0]) > 2  || (
                    Integer.parseInt(sw.getSwitchDescription().getSoftwareDescription().toLowerCase().split("\\.")[0]) == 2 &&
                    Integer.parseInt(sw.getSwitchDescription().getSoftwareDescription().toLowerCase().split("\\.")[1]) >= 1 ))
                    ){
	                    if(FLOWMOD_DEFAULT_MATCH_TCP_FLAG){
	                        mb.setExact(MatchField.OVS_TCP_FLAGS, U16.of(tcp.getFlags()));
	                    }
                    }
                } else if (ip.getNextHeader().equals(IpProtocol.UDP)) {
                    UDP udp = (UDP) ip.getPayload();
                    mb.setExact(MatchField.IP_PROTO, IpProtocol.UDP);
                    if (FLOWMOD_DEFAULT_MATCH_TRANSPORT_SRC) {
                        mb.setExact(MatchField.UDP_SRC, udp.getSourcePort());
                    }
                    if (FLOWMOD_DEFAULT_MATCH_TRANSPORT_DST) {
                        mb.setExact(MatchField.UDP_DST, udp.getDestinationPort());
                    }
                }
            }
        }
        return mb.build();
    }

	protected void doFlood(IOFSwitch sw, OFPacketIn pi,  FloodlightContext cntx) {
        OFPort inPort = OFMessageUtils.getInPort(pi);
        OFPacketOut.Builder pob = sw.getOFFactory().buildPacketOut();
        List<OFAction> actions = new ArrayList<OFAction>();
        Set<OFPort> broadcastPorts = this.topologyService.getSwitchBroadcastPorts(sw.getId());

        if (broadcastPorts.isEmpty()) {
            log.debug("No broadcast ports found. Using FLOOD output action");
            broadcastPorts = Collections.singleton(OFPort.FLOOD);
        }

        for (OFPort p : broadcastPorts) {
            if (p.equals(inPort)) continue;
            actions.add(sw.getOFFactory().actions().output(p, Integer.MAX_VALUE));
        }
        pob.setActions(actions);
        
        // set buffer-id, in-port and packet-data based on packet-in
        pob.setBufferId(OFBufferId.NO_BUFFER);
        OFMessageUtils.setInPort(pob, inPort);
        pob.setData(pi.getData());

        if (log.isTraceEnabled()) {
            log.trace("Writing flood PacketOut switch={} packet-in={} packet-out={}",
                    new Object[] {sw, pi, pob.build()});
        }
        messageDamper.write(sw, pob.build());

        return;
    }
	 
	@Override
	public Command processPacketInMessage(IOFSwitch sw, OFPacketIn pi,
				FloodlightContext cntx) {
		Ethernet eth = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
		if (eth.isBroadcast() || eth.isMulticast()) 
	            doFlood(sw, pi, cntx);
	    else 
	    	doForwardFlow(sw, pi, cntx);
		return Command.CONTINUE;
	}	
	
	@Override
	public Command processFlowRemovedMessage(IOFSwitch sw, OFFlowRemoved flowRemovedMessage) {
		U64 cookie = flowRemovedMessage.getCookie();
		if(AppCookie.extractApp(cookie) != DYNAMIC_ROUTING_APP_ID){
			return Command.CONTINUE;
		}
		
		log.info("{} flow entry removed {}", sw, flowRemovedMessage);
		
		//This kind of removed flows have processed at the reroute function
		if(flowRemovedMessage.getReason().equals(OFFlowRemovedReason.DELETE))
			return Command.CONTINUE;
		
		//including both normal and vip flows
		flowRegistry.removeExpiredFlow(cookie);  
		
		return Command.CONTINUE;
	}
	
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l =
				new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IThreadPoolService.class);
		l.add(IStatisticsService.class);
		l.add(ITopologyService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException {
		super.init();
		this.topologyService = context.getServiceImpl(ITopologyService.class);
		this.floodlightProviderService = context.getServiceImpl(IFloodlightProviderService.class);
        this.deviceManagerService = context.getServiceImpl(IDeviceService.class);
        this.routingEngineService = context.getServiceImpl(IRoutingService.class);
        this.topologyService = context.getServiceImpl(ITopologyService.class);
        this.switchService = context.getServiceImpl(IOFSwitchService.class);
		
		flowRegistry = FlowRegistry.getInstance();
		pathcache = new HashMap<>();
		nextIndex = new HashMap<>();
		
		Map<String, String> config = context.getConfigParams(this);
		if (config.containsKey(ENABLED_STR)) {
			try {
				isEnabled = Boolean.parseBoolean(config.get(ENABLED_STR).trim());
			} catch (Exception e) {
				log.error("Could not parse '{}'. Using default of {}", ENABLED_STR, isEnabled);
			}
		}
		log.info("Dynamic Routing {}", isEnabled ? "enabled" : "disabled");
		
	}

	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
		if(isEnabled){
			super.startUp();
			topologyService.addListener(this);
			new ServerThread().start();
//			threadPoolService.getScheduledExecutor().scheduleAtFixedRate(new TestReroute(),30,500,TimeUnit.SECONDS);
		}

	}
	
	private class ServerThread extends Thread{
		public void run(){
			log.info("server start");
			try(ServerSocket ss = new ServerSocket(30000)){
				while(true){
					Socket incoming = ss.accept();
					log.info("WebSocket connected");
					new MonitorThread(incoming).start();
				}
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}
	
	
	private class MonitorThread extends Thread{
		private Socket socket;
		
		public MonitorThread(Socket socket){
			this.socket = socket;
		}
		
		public void run(){
			try{
				try{
					shakeHand();
						
					while(true){
						String msg = receive();
						log.info("bufferLevel = " + msg);
						try{
							Double bufferLevel = Double.parseDouble(msg);
							if(bufferLevel < FLOOR){
								log.info("----------- emergency! --------");
								isEmergency = true;
								Iterator<Path> it = flowRegistry.vipPaths();
								while(it.hasNext())
									reroute(it.next());
								
							}else if(bufferLevel > CEILING)
								isEmergency = false;
							//send(getQualityForBitrate());	
						}
						catch(NumberFormatException e){
							log.info("something must be wrong");
						}

					}
				}finally{
					socket.close();
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		
		private void shakeHand() throws Exception{
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			byte[] buff = new byte[1024];
			int count = -1;
			String req = "";
			count = in.read(buff);
			req = new String(buff, 0, count);
			System.out.println("握手请求：" + req);
			
			String secKey = getSecWebSocketKey(req);
			System.out.println("secKey = " + secKey);
			String response = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: "
	                + "websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
	                + getSecWebSocketAccept(secKey) + "\r\n\r\n";
			System.out.println("secAccept = " + getSecWebSocketAccept(secKey));
	        out.write(response.getBytes());
	    }

		private String receive() throws Exception{
			InputStream in = socket.getInputStream();
	        byte[] buff = new byte[1024];
	        int count = -1;
	        count = in.read(buff);

	        for(int i = 0;i < count - 6;i++){
	        	buff[i+6] = (byte)(buff[i%4 + 2] ^ buff[i+6]);
	        }
	        return new String(buff, 6, count - 6, "UTF-8");
	    }
		
		/*
		private void send(String msg) throws Exception{
			OutputStream out = socket.getOutputStream();
	        byte[] pushHead = new byte[2];
	        pushHead[0] = -127;
	        pushHead[1] = (byte)msg.getBytes("UTF-8").length;
	        out.write(pushHead);
	        out.write(msg.getBytes("UTF-8"));
		}
		*/

		private String getSecWebSocketKey(String req){
	        Pattern p = Pattern.compile("^(Sec-Websocket-Key:).+",
	                Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
	        Matcher m = p.matcher(req);
	        if(m.find()){
	            String foundstring = m.group();
	            return foundstring.split(":")[1].trim();
	        }else{
	            return null;
	        }
	    }
		
		private String getSecWebSocketAccept(String secKey) throws Exception{
	        String guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

	        secKey += guid;

	        MessageDigest md = MessageDigest.getInstance("SHA-1");
	        md.update(secKey.getBytes("ISO-8859-1"),0,secKey.length());

	        byte[] sha1Hash = md.digest();

	        BASE64Encoder encoder = new BASE64Encoder();
	        return encoder.encode(sha1Hash);
	    }
	}
}

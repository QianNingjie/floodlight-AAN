package net.floodlightcontroller.qoedrivenadjustment;

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
import net.floodlightcontroller.linkdiscovery.Link;
import net.floodlightcontroller.packet.*;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Path;
import net.floodlightcontroller.routing.PathId;
import net.floodlightcontroller.sflowcollector.ISflowCollectionService;
import net.floodlightcontroller.sflowcollector.InterfaceStatistics;
import net.floodlightcontroller.statistics.IStatisticsService;
import net.floodlightcontroller.threadpool.IThreadPoolService;
import net.floodlightcontroller.topology.ITopologyService;
import net.floodlightcontroller.util.OFMessageUtils;
import org.projectfloodlight.openflow.protocol.*;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.*;
import org.sdnplatform.sync.internal.config.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class QoEdrivenAdjustment extends QoEdrivenAdjBase implements IFloodlightModule{
	private static final Logger log = LoggerFactory.getLogger(QoEdrivenAdjustment.class);
	
	private ITopologyService topologyService;

	
	private static boolean isEnabled = false;


	private Random rand = new Random(47);
	private List<IPv4Address> unsatClient;
	private static FlowRegistry flowRegistry;
	private static DatapathId serverAp;
	private static final String ENABLED_STR = "enable";
	private static final long FLOWSET_BITS = 52;
    private static final long FLOWSET_MAX = (long) (Math.pow(2, FLOWSET_BITS) - 1);

    private static final IPv4Address client = IPv4Address.of("192.168.56.11");
    private static final IPv4Address server = IPv4Address.of("192.168.56.12");
    private static final TransportPort serverPort = TransportPort.of(3000);
    private static final int CAPACITY = 10_000_000;
	private static final int VIDEO_BANDWIDTH = 2500_000;



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
		DatapathId dstSw = dstAp.getNodeId();
        OFPort dstPort = dstAp.getPortId();

        /* Validate that the source and destination are not on the same switch port */
        if (sw.getId().equals(dstAp.getNodeId()) && srcPort.equals(dstAp.getPortId())) {
        	if(srcSw.equals(DatapathId.of("00:00:00:00:00:00:00:01")) && srcPort.equals(OFPort.of(7)))
        		return;
        	log.info(srcSw.toString() + " vs " + DatapathId.of("00:00:00:00:00:00:00:02").toString() + "  "
        			+ srcPort.toString() + " vs " + OFPort.of(7).toString() );
            log.info("Both source and destination are on the same switch/port {}/{}. Dropping packet", sw.toString(), srcPort);
            return;
        }		
        
        //flag[0]:该流是否需要记录，只有tcp和udp流要被记录
        //flag[1]:该流是否是视频服务器到客户端的流
        Boolean[] flag = new Boolean[2]; 

        IPv4Address clientIp = null;
        Match m = createMatchFromPacket(sw, srcPort, pi, cntx, flag, clientIp);
        System.err.println(m);
        
        long user_fields = flowRegistry.generateFlowId(log);
        U64 cookie = AppCookie.makeCookie(QDA_APP_ID, user_fields);

		List<NodePortTuple> nptList = new ArrayList<>();
		NodePortTuple npt = new NodePortTuple(srcSw, srcPort);
		nptList.add(npt);
		List<NodePortTuple> woAp = null;
		if(!srcSw.equals(dstAp.getNodeId())) {
			List<Path> paths = routingEngineService.getPathsFast(srcSw, dstSw);
			woAp = paths.get(rand.nextInt(paths.size())).getPath();
			nptList.addAll(woAp);
		}
		npt = new NodePortTuple(dstSw, dstPort);
		nptList.add(npt);

		PathId pathId = new PathId(srcSw, dstSw);
		Path path = new Path(pathId, nptList);


        if (!path.getPath().isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("pushRoute inPort={} route={} " +"destination={}:{}",
                        new Object[] { srcPort, path,dstAp.getNodeId(),  dstPort});
                log.debug("Creating flow rules on the route, match rule: {}", m);
            }

            pushRoute(path, m, pi, sw.getId(), cookie, flag[0], OFFlowModCommand.ADD);	
  
            
            //This is done after we push the path as it is blocking.
            if(woAp == null || !flag[0])
            	return;

            flowRegistry.register(cookie, m, path, woAp, flag[1] ? 0 : 1, clientIp);
        } 
    }


    protected Match createMatchFromPacket(IOFSwitch sw, OFPort inPort, OFPacketIn pi, FloodlightContext cntx, Boolean[] flag, IPv4Address clinetIp) {
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
					flag[0] = true;
                    TCP tcp = (TCP) ip.getPayload();
                    mb.setExact(MatchField.IP_PROTO, IpProtocol.TCP);
                    if(srcIp.equals(server) && tcp.getSourcePort().equals(serverPort)){
                    	flag[1] =true;
                    	mb.setExact(MatchField.TCP_SRC, tcp.getSourcePort());
                    	clinetIp = dstIp;
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
		if(AppCookie.extractApp(cookie) != QDA_APP_ID){
			return Command.CONTINUE;
		}
		
		log.info("{} flow entry removed {}", sw, flowRemovedMessage);
		
		//This kind of removed flows has processed at the reroute function
		if(flowRemovedMessage.getReason().equals(OFFlowRemovedReason.DELETE))
			return Command.CONTINUE;
		
		//including both ordinary and vip flows
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
		l.add(ITopologyService.class);
		l.add(ISflowCollectionService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException {
		super.init();
		this.topologyService = context.getServiceImpl(ITopologyService.class);
		this.floodlightProviderService = context.getServiceImpl(IFloodlightProviderService.class);
        this.deviceManagerService = context.getServiceImpl(IDeviceService.class);
        this.routingEngineService = context.getServiceImpl(IRoutingService.class);
        this.switchService = context.getServiceImpl(IOFSwitchService.class);
		this.sflowCollectionService = context.getServiceImpl(ISflowCollectionService.class);
		this.threadPoolService = context.getServiceImpl(IThreadPoolService.class);
		
		flowRegistry = FlowRegistry.getInstance();
		unsatClient = Collections.synchronizedList(new ArrayList<>());

		Map<String, String> config = context.getConfigParams(this);
		if (config.containsKey(ENABLED_STR)) {
			try {
				isEnabled = Boolean.parseBoolean(config.get(ENABLED_STR).trim());
			} catch (Exception e) {
				log.error("Could not parse '{}'. Using default of {}", ENABLED_STR, isEnabled);
			}
		}
		log.info("QoE-driven Adjustment {}", isEnabled ? "enabled" : "disabled");
		
	}

	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
		if(isEnabled){
			super.startUp();
			new Thread(new ComplainCollecter(unsatClient)).start();
			threadPoolService.getScheduledExecutor().scheduleAtFixedRate(new Adjustment(),20,5, TimeUnit.SECONDS);
		}

	}


    private class Adjustment implements Runnable{
        @Override
        public void run() {
            Map<DatapathId, Set<Link>> dpidToLinks = topologyService.getAllLinks();
            int n = dpidToLinks.keySet().size();
            if(n == 0) return;

            Set<Link> links = new HashSet<>();
            for(Set<Link> set : dpidToLinks.values())
                links.addAll(set);

            Map<NodePortTuple,InterfaceStatistics> statisticsMap = sflowCollectionService.getStatisticsMap();
            Map<Link, Integer> linkIdle = new HashMap<>();
            Map<Link, Integer> linkBg = new HashMap<>();
            for(Link link : links){
                int outRate = (int)statisticsMap.get(new NodePortTuple(link.getSrc(),link.getSrcPort())).getIfOutOctets().doubleValue() * 8 ; //byte/s
                linkIdle.put(link, CAPACITY - outRate);
                int cnt = flowRegistry.linkToFlow.get(link)[0].size();
                linkBg.put(link, outRate - VIDEO_BANDWIDTH * cnt);
            }

            int flowSrc = (int)serverAp.getLong();
            List<Integer> flowDst = new ArrayList<>();
            for(IPv4Address ip : unsatClient){
                U64 cookie = flowRegistry.ipToCki.get(ip);
                flowDst.add((int)flowRegistry.flowToPath.get(cookie).getPath().get(0).getNodeId().getLong());
            }

            int threshold = 0;
            List<List<Link>> flowPath = new ArrayList<>();
            Map<Link, Integer> linkLimit = new HashMap<>();
            MaxFlowSolver.rearrangeFlow(links, n, linkIdle, linkBg, flowSrc, flowDst, VIDEO_BANDWIDTH, threshold, flowPath, linkLimit);

            for(int i = 0; i < unsatClient.size(); i++){
                U64 cookie = flowRegistry.ipToCki.get(unsatClient.get(i));
                List<NodePortTuple> oldPath = flowRegistry.flowToPath.get(cookie).getPath();
                NodePortTuple first = oldPath.get(0), last = oldPath.get(oldPath.size()-1);

                List<Link> linkList = flowPath.get(i);
                List<NodePortTuple> nptList = new ArrayList<>();
                nptList.add(first);
                NodePortTuple npt1, npt2;
                for(Link link : linkList){
                    npt1 = new NodePortTuple(link.getSrc(),link.getSrcPort());
                    npt2 = new NodePortTuple(link.getSrc(),link.getSrcPort());
                    nptList.add(npt1);
                    nptList.add(npt2);
                }
                nptList.add(last);
                PathId pathId = new PathId(first.getNodeId(),last.getNodeId());
                Path path = new Path(pathId, nptList);
                pushRoute(path, flowRegistry.flowMatch.get(cookie), null, null, cookie, true, OFFlowModCommand.ADD);

                removeFlow(flowRegistry.flowToPath.get(cookie), cookie);

                flowRegistry.update(cookie, oldPath, linkList, path);

            }

            unsatClient.clear();
        }
    }


}

package net.floodlightcontroller.qoedrivenadjustment;

import net.floodlightcontroller.core.types.NodePortTuple;
import net.floodlightcontroller.linkdiscovery.Link;
import net.floodlightcontroller.routing.Path;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.U64;
import org.slf4j.Logger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by ningjieqian on 17/7/9.
 */
public class FlowRegistry {
    private static final long FLOWSET_BITS = 52;
    private static final long FLOWSET_MAX = (long) (Math.pow(2, FLOWSET_BITS) - 1);

    volatile Map<U64, Path> flowToPath;
    volatile Map<Link, Set<U64>[]> linkToFlow; //0视频流
    volatile Map<U64, Match> flowMatch;
    volatile Map<IPv4Address, U64> ipToCki; //只记录从视频服务器到客户端的流的cookie（反向流比较小，不需要保障带宽）

    private volatile long flowIdGenerator = -1;
    private static volatile FlowRegistry instance;

    private FlowRegistry(){
        flowToPath  = new ConcurrentHashMap<>();
        linkToFlow = new ConcurrentHashMap<>();
        flowMatch = new ConcurrentHashMap<>();
        ipToCki = new ConcurrentHashMap<>();


    }

     static FlowRegistry getInstance() {
        if (instance == null)
            instance = new FlowRegistry();
        return instance;
    }

     synchronized long generateFlowId(Logger log) {
        flowIdGenerator += 1;
        if (flowIdGenerator == FLOWSET_MAX) {
            flowIdGenerator = 0;
            log.warn("Flowset IDs have exceeded capacity of {}. Flowset ID generator resetting back to 0", FLOWSET_MAX);
        }
        log.debug("Generating flowset ID {}", flowIdGenerator);
        return flowIdGenerator;
    }

    void register(U64 cookie, Match match, Path path, List<NodePortTuple> npts, int type, IPv4Address clientIp){
        flowMatch.put(cookie, match);
        if(clientIp != null)
            ipToCki.put(clientIp, cookie);

        flowToPath.put(cookie, path);

        for(int i = 0; i < npts.size()-1; i++){
            NodePortTuple a = npts.get(i);
            NodePortTuple b = npts.get(i+1);
            Link link = new Link(a.getNodeId(), a.getPortId(), b.getNodeId(), b.getPortId(), U64.ZERO);
            addToLtf(link, cookie, type);
        }
    }

    private void addToLtf(Link link, U64 cookie, int type){
        if(linkToFlow.containsKey(link))
            linkToFlow.get(link)[type].add(cookie);
        else{
            Set[] arr = new Set[2];
            arr[0] = new HashSet<U64>();
            arr[1] = new HashSet<U64>();
            arr[type].add(cookie);
            linkToFlow.put(link,arr);
        }
    }

    void update(U64 cookie, List<NodePortTuple> oldPath, List<Link> linkList, Path newPath){
        flowToPath.put(cookie, newPath);

        //remove vip flow from old links
        for(int i = 1; i < oldPath.size()-1; i += 2){
            NodePortTuple npt1 = oldPath.get(i);
            NodePortTuple npt2 = oldPath.get(i+1);
            Link link = new Link(npt1.getNodeId(),npt1.getPortId(), npt2.getNodeId(), npt2.getPortId(), U64.ZERO);
            boolean flag = linkToFlow.get(link)[0].remove(cookie);
            if(!flag) throw new RuntimeException("what happened?");
        }

        //add new
        for(Link link :linkList)
            addToLtf(link, cookie, 0);
    }


    void removeExpiredFlow(U64 cookie){
        flowToPath.remove(cookie);
        flowMatch.remove(cookie);

        Iterator<Map.Entry<Link, Set<U64>[]>> it = linkToFlow.entrySet().iterator();
        while(it.hasNext()){
            Set<U64>[] sets = it.next().getValue();
            sets[0].remove(cookie);
            sets[1].remove(cookie);
        }

    }
}
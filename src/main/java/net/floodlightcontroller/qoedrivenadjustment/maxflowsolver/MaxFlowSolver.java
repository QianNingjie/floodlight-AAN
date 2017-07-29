package net.floodlightcontroller.qoedrivenadjustment.maxflowsolver;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by ningjieqian on 17/7/17.
 */
public class MaxFlowSolver {
    private class Edge{
        int y,c,next;
        Edge(int y, int c, int next){
            this.y = y;
            this.c = c;
            this.next = next;
        }
    }

    Edge[] e ;
    int[] head ;
    int nn, mm;
    int[] d , cont , q ;
    int[] pre , cur ;
    boolean[] vis ;


    void bfs(int s){
        int x, to, tail = 1, front = 0;
        for(int i = 1; i <=nn; i++) {
            vis[i] = false;
            cont[i] = 0;
            cur[i] = head[i];
            d[i] = 0x3fffffff;
        }
        d[s] = 0; cont[0] = 1; q[0] = s; vis[s] = true;
        while(front < tail)
            for(int i = head[x=q[front++]]; i != -1; i = e[i].next)
                if(!vis[to=e[i].y] && e[i^1].c != 0){
                    d[to] = d[x] + 1;
                    vis[to] = true;
                    q[tail++] = to;
                    cont[d[to]]++;
                }
    }

    int SAP(int s, int t){
        if(s==t) return 0x7fffffff;
        bfs(t);
        pre[s] = -1;
        int ans = 0, x = s, y ,flow, back = 0;
        while(d[s] < nn)
        {
            y = -1;
            for(int i = cur[x]; i != -1; i = e[i].next)
                if(e[i].c != 0 && d[x] == d[e[i].y] + 1){
                    y = e[i].y;
                    cur[x] = i;
                    break;
                }
            if(y != -1){
                    pre[y] = x; x = y;
                    if(x == t){
                        flow = 0x3fffffff;
                        for(y = pre[y]; y != -1; y = pre[y])
                            if(flow >= e[cur[y]].c){
                                flow = e[cur[y]].c;
                                back = y;
                        }
                        for(x = pre[x]; x != -1; x = pre[x]){
                            e[cur[x]^1].c += flow;
                            e[cur[x]].c -= flow;
                        }
                        ans += flow; x = back;
                    }
            }else{
                y = nn;
                for(int i = head[x]; i != -1; i = e[i].next)
                    if(e[i].c != 0&& y > d[e[i].y]){
                        y = d[e[i].y];
                        cur[x] = i;
                }
                cont[d[x]]--;
                if(cont[d[x]] == 0) break;
                cont[d[x]=y+1]++;
                if(x != s) x = pre[x];
            }
        }
        return ans;

    }

    //x,y：点编号，c：边容量
    void addLink(int x, int y, int c){
        e[mm] = new Edge(y,c,head[x]);head[x]=mm++;
        e[mm] = new Edge(x,0,head[y]);head[y]=mm++;
    }

    void initGraph(int n, int m){
        e =  new Edge[m * 4];
        int rn = n + 1;
        head = new int[rn];
        d = new int[rn];
        cont = new int[rn];
        q = new int[rn];
        pre = new int[rn];
        cur = new int[rn];
       vis = new boolean[rn];
        for(int i = 1; i <= n; i++)
            head[i] = -1;
        nn = n;
        mm = 0;
    }

    void findPath(int s, List<Link> path){
        for(int i = head[s]; ~i != 0; i = e[i].next)
            if((i & 1) != 0 && e[i].c != 0){
                e[i].c--;
                findPath(e[i].y, path);
                path.add(edgeIndexTable.get(i >> 1));
                return;
            }
    }

    int nodeNum, edgeNum;
    int destination;
    Map<Integer, Integer> nodeTable;  //读入的src编号 -> 从0开始连续编号
    Map<Link, Integer> edgeTable;
    List<Link> edgeIndexTable;

    int flowSrc;
    int[] flowDst;
    int dstCnt;

    int[] src, dst, restCap, bgCap, used;

    void init(Map<Integer, Set<Link>> links, int n, Map<Link, Integer> linkIdle,
              Map<Link, Integer> linkBg, int flowSrc, List<Integer> flowDst){

        nodeTable = new HashMap<>();
        edgeTable = new HashMap<>();
        edgeIndexTable = new ArrayList<>();
        edgeNum = linkBg.size();
        src = new int[edgeNum];
        dst = new int[edgeNum];
        restCap = new int[edgeNum];
        bgCap = new int[edgeNum];
        used = new int[edgeNum];
        this.flowDst = new int[flowDst.size()];

        nodeNum = 0;
        edgeNum = 0;
        for(Set<Link> set : links.values())
            for(Link link : set){
                if(!nodeTable.containsKey(link.src))
                    nodeTable.put(link.src, ++nodeNum);
                if(!nodeTable.containsKey(link.dst))
                    nodeTable.put(link.dst, ++nodeNum);

                edgeIndexTable.add(link);
                edgeTable.put(link,edgeNum);

                src[edgeNum] = nodeTable.get(link.src);
                dst[edgeNum] = nodeTable.get(link.dst);
                edgeNum++;
            }


        for(Map.Entry<Link, Integer> entry : linkIdle.entrySet())
            restCap[edgeTable.get(entry.getKey())] = entry.getValue();

        for(Map.Entry<Link, Integer> entry : linkBg.entrySet())
            bgCap[edgeTable.get(entry.getKey())] = entry.getValue();

        dstCnt = 0;
        for(Integer x : flowDst)
            this.flowDst[dstCnt++] = nodeTable.get(x);
        this.flowSrc = nodeTable.get(flowSrc);
    }

    private boolean solve(int bandwidth, int threshold, int padding, List<List<Link>> flowPath, Map<Link, Integer> linkLimit) {
        if(padding < 0){
            initGraph(nodeNum + 1, edgeNum + dstCnt);
            for(int i = 0; i < edgeNum; i++){
                int rest = Math.max(restCap[i] + padding - threshold, 0);
                addLink(src[i], dst[i], rest / bandwidth);
            }
            int sink = nodeNum + 1;
            for(int i = 0; i < dstCnt; i++)
                addLink(flowDst[i], sink, 1);
            int flow = SAP(flowSrc, sink);
            if(flow == dstCnt){
                linkLimit.clear();
                for(int i = 0; i < edgeNum; i++){
                    int use = e[2*i+1].c * bandwidth + threshold;
                    int limit = Math.max(0, use - restCap[i]);
                    if(limit != 0)
                        linkLimit.put(edgeIndexTable.get(i), limit);
                }

                flowPath.clear();
                for(int i = 0; i < dstCnt; i++){
                    List<Link> path = new ArrayList<>();
                    findPath(flowDst[i], path);
                    flowPath.add(path);
                }
                return true;
            }else
                return false;

        }else{
            initGraph(nodeNum + 1, edgeNum + dstCnt);
            for(int i = 0; i < edgeNum; i++){
                int rest = restCap[i] + Math.min(padding, bgCap[i] - threshold);
                addLink(src[i], dst[i], rest / bandwidth);
            }
            int sink = nodeNum + 1;
            for(int i = 0; i < dstCnt; i++)
                addLink(flowDst[i], sink, 1);
            int flow = SAP(flowSrc, sink);
            if(flow == dstCnt){
                linkLimit.clear();
                for(int i = 0; i < edgeNum; i++){
                    int use = e[2*i+1].c * bandwidth + threshold;
                    int limit = Math.max(0, use - restCap[i]);
                    if(limit != 0)
                        linkLimit.put(edgeIndexTable.get(i), limit);
                }

                flowPath.clear();
                for(int i = 0; i < dstCnt; i++){
                    List<Link> path = new ArrayList<>();
                    findPath(flowDst[i], path);
                    flowPath.add(path);
                }
                return true;
            }else
                return false;
        }
    }

    private static boolean rearrangeFlow(Map<Integer, Set<Link>> links, int n, Map<Link, Integer> linkIdle,
                                         Map<Link, Integer> linkBg, int flowSrc, List<Integer> flowDst,
                                         int bandwidth, int threshold, List<List<Link>> flowPath, Map<Link, Integer> linkLimit) {
        int maxBandwidth = 0, minBandwidth = 0;
        for(Integer bg : linkBg.values())
            maxBandwidth = Math.max(maxBandwidth, bg);

        for(Integer idle : linkIdle.values())
            minBandwidth = Math.max(minBandwidth, idle);

        MaxFlowSolver solver = new MaxFlowSolver();
        solver.init(links, n, linkIdle, linkBg, flowSrc, flowDst);

        if(!solver.solve(bandwidth, threshold, maxBandwidth, flowPath, linkLimit))
            return false;

        int l = -minBandwidth, r = maxBandwidth, mid;
        while(l < r){
            mid = (l + r) / 2;
            if(solver.solve(bandwidth, threshold, mid, flowPath, linkLimit))
                r = mid;
            else
                l = mid + 1;
        }

        System.err.println(r);
        solver.solve(bandwidth, threshold, r, flowPath, linkLimit);
        return true;
    }



    public static void main(String[] args) throws Exception{
        File f1 = new File("/Users/ningjieqian/Downloads/temp/2.txt");
        File f2 = new File("/Users/ningjieqian/Desktop//javaout/1_out.txt");
        Scanner in = new Scanner(f1);
        PrintWriter out = new PrintWriter(f2);
        Map<Link, Integer> linkCp = new HashMap<>(), linkBg = new HashMap<>();
        Map<Integer, Set<Link>> links = new HashMap<>();

        int n = in.nextInt();
        int m = in.nextInt();
        for(int i = 0; i < m; i++){
            int a = in.nextInt();
            int b = in.nextInt();
            int c = in.nextInt();
            int d = in.nextInt();
            Link link = new Link(a,b,c,d);
            if(links.containsKey(a))
                links.get(a).add(link);
            else{
                Set<Link> set = new HashSet<>();
                set.add(link);
                links.put(a, set);
            }
            a = in.nextInt();
            b = in.nextInt();
            linkCp.put(link, a-b);
            linkBg.put(link, b);
        }

        List<Integer> flowDst = new ArrayList<>();
        int cnt = in.nextInt();
        for(int i = 0; i < cnt; i++)
            flowDst.add(in.nextInt());

        int flowSrc = in.nextInt();
        int bandwidth = in.nextInt();
        List<List<Link>> flowPath = new ArrayList<>();
        Map<Link, Integer> linkLimit = new HashMap<>();
        rearrangeFlow(links, n, linkCp, linkBg, flowSrc, flowDst, bandwidth, 0, flowPath, linkLimit);
        for(List<Link> path : flowPath)
            out.println(path);
        out.println("**************");


        for(Link link : linkLimit.keySet())
            out.println(link + " " + linkLimit.get(link));
        in.close();
        out.close();

    }

}

class Link{
    int src;
    int srcPort;
    int dst;
    int dstPort;

    public Link(int a, int b, int c, int d){
        src = a;
        srcPort = b;
        dst = c;
        dstPort = d;
    }

    public String toString(){
        return src + " " + srcPort + " " + dst + " " + dstPort;
    }
}
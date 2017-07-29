package net.floodlightcontroller.sflowcollector;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.lang.time.DateFormatUtils;
import org.json.JSONException;
import org.json.JSONObject;

public class InterfaceStatistics {
	public static final String ALLJSONPROPERTIES = "ifdirection ifinmulticastpkts ifinpkts ifindex ifindiscards ifinerrors ifinoctets ifinutilization ifinmutilcastpkts ifoutdiscards ifouterrors ifoutoctets ifoututilization ifoutpkts ifspeed iftype";
	
	//注释部分属于自定义及比较重要的端口参数
	private Integer ifIndex;  // sflow中的端口号
	private Integer port; // 交换机真正端口号
	private String ifDirection; //端口工作模式 双工...
	private String ifName;
	private String ifType;
	private Double ifSpeed;
	private Double ifInMulticastpkts;
	private Double ifInpkts;
	private Double ifInOctets; //接收速率
	private Double ifInUtilization;
	private Double ifInDiscards;
	private Double ifInErrors;
	private Double ifOutpkts;
	private Double ifOutOctets; // 发送速率
	private Double ifOutUtilization;
	private Double ifOutDiscards;
	private Double ifOutErrors;
	private String time;
	public InterfaceStatistics() { }
	
	public InterfaceStatistics(Integer ifIndex, String ifName) {
		this.ifIndex = ifIndex;
		this.ifName = ifName;
		SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		this.time=df.format(new Date());
	}	
	public InterfaceStatistics(Integer ifIndex, String ifName, String ifType,
			Double ifSpeed, Double ifInMulticastpkts, Double ifInpkts,
			Double ifInOctets, Double ifInUtilization, Double ifInDiscards, Double ifInErrors,
			Double ifOutpkts, Double ifOutOctets,Double ifOutUtilization, Double ifOutDiscards,
			Double ifOutErrors) {
		super();
		this.ifIndex = ifIndex;
		this.ifName = ifName;
		this.ifType = ifType;
		this.ifSpeed = ifSpeed;
		this.ifInMulticastpkts = ifInMulticastpkts;
		this.ifInpkts = ifInpkts;
		this.ifInOctets = ifInOctets;
		this.ifInUtilization = ifInUtilization;
		this.ifInDiscards = ifInDiscards;
		this.ifInErrors = ifInErrors;
		this.ifOutpkts = ifOutpkts;
		this.ifOutOctets = ifOutOctets;
		this.ifOutUtilization = ifOutUtilization;
		this.ifOutDiscards = ifOutDiscards;
		this.ifOutErrors = ifOutErrors;
		SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		this.time=df.format(new Date());
	}
	
	public void fromJsonProp(String jsonProp, JSONObject jo) {
		if(jsonProp == null || jsonProp.length() == 0) return;
		if(jo == null || !jo.has(jsonProp)) return;
		time=DateFormatUtils.format(new Date(),
				"yyyy/MM/dd/ HH:mm:ss");	
		try {
			if (jsonProp.contains("iftype")) {
				ifType = jo.getString(jsonProp);
			} else if (jsonProp.contains("ifspeed")) {
				ifSpeed = jo.getDouble(jsonProp);
			} else if (jsonProp.contains("ifinmulticastpkts")) {
				ifInMulticastpkts = jo.getDouble(jsonProp);
			} else if (jsonProp.contains("ifinpkts")) {
				ifInpkts = jo.getDouble(jsonProp);
			} else if (jsonProp.contains("ifinoctets")) {
				ifInOctets = jo.getDouble(jsonProp);
			} else if (jsonProp.contains("ifinutilization")) {
				ifInUtilization = jo.getDouble(jsonProp);
			} else if (jsonProp.contains("ifindiscards")) {
				ifInDiscards = jo.getDouble(jsonProp);
			} else if (jsonProp.contains("ifinerrors")) {
				ifInErrors = jo.getDouble(jsonProp);
			} else if (jsonProp.contains("ifoutpkts")) {
				ifOutpkts = jo.getDouble(jsonProp);
			} else if (jsonProp.contains("ifoutoctets")) {
				ifOutOctets = jo.getDouble(jsonProp);
			} else if (jsonProp.contains("ifoututilization")) {
				ifOutUtilization = jo.getDouble(jsonProp);
			} else if (jsonProp.contains("ifoutdiscards")) {
				ifOutDiscards = jo.getDouble(jsonProp);
			} else if (jsonProp.contains("ifouterrors")) {
				ifOutErrors = jo.getDouble(jsonProp);
			}else if(jsonProp.contains("ifdirection")) {
				ifDirection = jo.getString(jsonProp);
			}
						
		} catch (JSONException e) {
			return;
		}
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[ifIndex=" + ifIndex);
		sb.append(", ifdirection=" + ifDirection);
		sb.append(", port=" + port);
		sb.append(", ifName=" + ifName);
		sb.append(", ifType=" + ifType);
		sb.append(", ifSpeed=" + ifSpeed);
		sb.append(", ifInMulticastpkts=" + ifInMulticastpkts);
		sb.append(", ifInUcastpkts=" + ifInpkts);
		sb.append(", ifInOctets=" + ifInOctets);
		sb.append(", ifInUtilization=" +ifInUtilization);
		sb.append(", ifInDiscards=" + ifInDiscards);
		sb.append(", ifInErrors=" + ifInErrors);
		sb.append(", ifOutUcastpkts=" + ifOutpkts);
		sb.append(", ifOutOctets=" + ifOutOctets);
		sb.append(", ifOutUtilization=" +ifOutUtilization);
		sb.append(", ifOutDiscards=" + ifOutDiscards);
		sb.append(", ifOutErrors=" + ifOutErrors + "]");
		return sb.toString();
	}

	public String getifDirection() {
		return ifDirection;
	}
	public Integer getport() {
		return port;
	}
	public Integer getIfIndex() {
		return ifIndex;
	}
	public String getIfName() {
		return ifName;
	}
	public String getIfType() {
		return ifType;
	}
	public Double getIfSpeed() {
		return ifSpeed;
	}
	public Double getIfInMulticastpkts() {
		return ifInMulticastpkts;
	}
	public Double getIfInpkts() {
		return ifInpkts;
	}
	public Double getIfInOctets() {
		return ifInOctets;
	}
	public Double getifInUtilization(){
		return ifInUtilization;
	}
	public Double getIfInDiscards() {
		return ifInDiscards;
	}
	public Double getIfInErrors() {
		return ifInErrors;
	}
	public Double getIfOutpkts() {
		return ifOutpkts;
	}
	public Double getIfOutOctets() {
		return ifOutOctets;
	}
	public Double getifOutUtilization(){
		return ifOutUtilization;
	}
	public Double getIfOutDiscards() {
		return ifOutDiscards;
	}
	public Double getIfOutErrors() {
		return ifOutErrors;
	}
	public String getTime(){
		return time;
	}
	
	// set
	public void setport(Integer port) {
		this.port = port;
	}
	public void setifDirection(String ifDirection) {
		this.ifName = ifDirection;
	}
	public void setIfIndex(Integer ifIndex) {
		this.ifIndex = ifIndex;
	}
	public void setIfName(String ifName) {
		this.ifName = ifName;
	}
	public void setIfType(String ifType) {
		this.ifType = ifType;
	}
	public void setIfSpeed(Double ifSpeed) {
		this.ifSpeed = ifSpeed;
	}
	public void setIfInMulticastpkts(Double ifInMulticastpkts) {
		this.ifInMulticastpkts = ifInMulticastpkts;
	}
	public void setIfInpkts(Double ifInpkts) {
		this.ifInpkts = ifInpkts;
	}
	public void setIfInOctets(Double ifInOctets) {
		this.ifInOctets = ifInOctets;
	}
	public void setifInUtilization(Double ifInUtilization){
		this.ifInUtilization = ifInUtilization;
	}
	public void setIfInDiscards(Double ifInDiscards) {
		this.ifInDiscards = ifInDiscards;
	}
	public void setIfInErrors(Double ifInErrors) {
		this.ifInErrors = ifInErrors;
	}
	public void setIfOutpkts(Double ifOutpkts) {
		this.ifOutpkts = ifOutpkts;
	}
	public void setIfOutOctets(Double ifOutOctets) {
		this.ifOutOctets = ifOutOctets;
	}
	public void setifOutUtilization(Double ifOutUtilization){
		this.ifOutUtilization = ifOutUtilization;
	}
	public void setIfOutDiscards(Double ifOutDiscards) {
		this.ifOutDiscards = ifOutDiscards;
	}
	public void setIfOutErrors(Double ifOutErrors) {
		this.ifOutErrors = ifOutErrors;
	}
	public void setTime(String time){
		this.time=time;
	}
}

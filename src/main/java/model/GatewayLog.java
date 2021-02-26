package model;

import java.util.Date;

public class GatewayLog {
	Date timestamp;
	String apiUrl,type, microservice ;

	public GatewayLog(Date timestamp, String apiUrl, String type, String microservice) {
		super();
		this.timestamp = timestamp;
		this.apiUrl = apiUrl;
		this.type = type;
		this.microservice = microservice;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public String getApiUrl() {
		return apiUrl;
	}

	public void setApiUrl(String apiUrl) {
		this.apiUrl = apiUrl;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getMicroservice() {
		return microservice;
	}

	public void setMicroservice(String microservice) {
		this.microservice = microservice;
	}
	
	
}

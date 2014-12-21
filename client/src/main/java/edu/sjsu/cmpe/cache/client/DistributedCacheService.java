package edu.sjsu.cmpe.cache.client;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

/**
 * Distributed cache service
 * 
 */
public class DistributedCacheService implements CacheServiceInterface {
	private final String cacheServerUrl;

	ConcurrentHashMap<String, String> status = new ConcurrentHashMap<String, String>();

	CRDTClient crdt;

	public DistributedCacheService(String serverUrl) {
		this.cacheServerUrl = serverUrl;
	}

	public DistributedCacheService(String serverUrl, ConcurrentHashMap<String, String> status) {
		this.cacheServerUrl = serverUrl;
		this.status = status;
	}

	public DistributedCacheService(String serverUrl, CRDTClient crdt) {
		this.cacheServerUrl = serverUrl;
		this.crdt = crdt;
	}

	public String getCacheServerURL(){
		return this.cacheServerUrl;
	}
	/**
	 * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#get(long)
	 */
	@Override
	public void get(long key) {
		Future<HttpResponse<JsonNode>> future = Unirest.get(this.cacheServerUrl + "/cache/{key}")
				.header("accept", "application/json")
				.routeParam("key", Long.toString(key))
				.asJsonAsync(new Callback<JsonNode>() {

					public void failed(UnirestException e) {
						System.out.println("The get request has failed");
						crdt.getStatus.put(cacheServerUrl, "fail");
					}

					public void completed(HttpResponse<JsonNode> response) {
						if(response.getCode() != 200) {
							crdt.getStatus.put(cacheServerUrl, "a");
						} else {
							String value = response.getBody().getObject().getString("value");
							System.out.println("Get value from server: "+cacheServerUrl+": "+value);
							crdt.getStatus.put(cacheServerUrl, value);
						}
					}

					public void cancelled() {
						System.out.println("The get request has been cancelled");
						crdt.getStatus.put(cacheServerUrl, "fail");
					}

				});
	}

	/**
	 * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#put(long,
	 *      java.lang.String)
	 */
	@Override
	public void put(long key, String value) {
		System.out.println("Sending key, value in put:"+key+", "+value);
		Future<HttpResponse<JsonNode>> future = Unirest.put(this.cacheServerUrl + "/cache/{key}/{value}")
				.header("accept", "application/json")
				.routeParam("key", Long.toString(key))
				.routeParam("value", value)
				.asJsonAsync(new Callback<JsonNode>() {

					public void failed(UnirestException e) {
						System.out.println("The put request has failed");
						crdt.putStatus.put(cacheServerUrl, "fail");
					}

					public void completed(HttpResponse<JsonNode> response) {
						if (response == null || response.getCode() != 200) {
							System.out.println("Failed to add to the cache.");
							crdt.putStatus.put(cacheServerUrl, "fail");
						} else {
							System.out.println("The put request is successfull");
							crdt.putStatus.put(cacheServerUrl, "pass");
						}
					}

					public void cancelled() {
						System.out.println("The put request has been cancelled");
						crdt.putStatus.put(cacheServerUrl, "fail");
					}

				});
	}

	/**
	 * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#delete(long)
	 */
	@Override
	public boolean delete(long key) {
		HttpResponse<JsonNode> response = null;
		try {
			response = Unirest.delete(this.cacheServerUrl + "/cache/{key}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key)).asJson();
		} catch (UnirestException e) {
			System.err.println(e);
		}

		if(response ==null || response.getCode() != 204) {
			System.out.println("Failed to perform delete operation.");
			return false;
		} else{
			System.out.println("Successfully performed delete operation.");
			return true;
		}
	}
}

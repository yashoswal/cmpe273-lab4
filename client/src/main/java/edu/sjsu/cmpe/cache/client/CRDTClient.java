package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class CRDTClient {
	
	public ConcurrentHashMap<String, String> putStatus = new ConcurrentHashMap<String, String>();
	public ConcurrentHashMap<String, String> getStatus = new ConcurrentHashMap<String, String>();
	private ArrayList<DistributedCacheService> servers = new ArrayList<DistributedCacheService>();
	
	public void addServer(String serverURL) {
		servers.add(new DistributedCacheService(serverURL,this));
	}
	
	
	public void put(long key, String value) {
		for(DistributedCacheService ser: servers) {
			ser.put(key, value);
		}
		
		while(true) {
        	if(putStatus.size() < 3) {
        		try {
        			System.out.println("Waiting put requests to be processed on all three servers");
					Thread.sleep(500);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	} else{
        		int fail = 0;
        		int pass = 0;
        		for(DistributedCacheService ser: servers) {
        			System.out.println("put status for : "+ser.getCacheServerURL()+": "+putStatus.get(ser.getCacheServerURL()));
        			if(putStatus.get(ser.getCacheServerURL()).equalsIgnoreCase("fail")) 
            			fail++;
            		else
            			pass++;
        		}
        		
        		if(fail > 1) {
        			System.out.println("Put failed on 2 or more servers. Rolling back changes");
        			for(DistributedCacheService ser: servers) {
        				ser.delete(key);
        			}
        		} else {
        			System.out.println("Successfully put the values");
        		}
        		putStatus.clear();
        		break;
        	}
        }
	}
	
	public String get(long key){
		for(DistributedCacheService ser: servers) {
			ser.get(key);
		}
		
		while(true) {
        	if(getStatus.size() < 3) {
        		try {
        			System.out.println("Waiting to get all the requests ");
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	} else{
        		HashMap<String, List<String>> getResults = new HashMap<String, List<String>>();
        		for(DistributedCacheService ser: servers) {
        			if(getStatus.get(ser.getCacheServerURL()).equalsIgnoreCase("fail")) 
            			System.out.println("Cannot get value from server: "+ser.getCacheServerURL());
            		else {
            			if(getResults.containsKey(getStatus.get(ser.getCacheServerURL()))) {
            				getResults.get(getStatus.get(ser.getCacheServerURL())).add(ser.getCacheServerURL());
            			} else {
            				List<String> temp = new ArrayList<String>();
            				temp.add(ser.getCacheServerURL());
            				getResults.put(getStatus.get(ser.getCacheServerURL()),temp);
            			}
            		}
        		}
        		
        		if(getResults.size() != 1) {
        			System.out.println("Values not consistent on all servers");
        			Iterator<Entry<String, List<String>>> it = getResults.entrySet().iterator();
        			int majority = 0;
        			String finalValue = null;
        			ArrayList <String> updateServer = new ArrayList<String>();
        		    while (it.hasNext()) {
        		        Map.Entry<String, List<String>> pairs = (Map.Entry<String, List<String>>)it.next();
        		        if(pairs.getValue().size() > majority) {
        		        	majority = pairs.getValue().size();
        		        	finalValue = pairs.getKey();
        		        } else {
        		        	for (String s: pairs.getValue()){
        		        		updateServer.add(s);
        		        	}
        		        }
        		    }
        		    
        			System.out.println("Making all values the servers consistent.");
        			for(String s: updateServer){
        				for(DistributedCacheService ser: servers) {
            				if(ser.getCacheServerURL().equalsIgnoreCase(s)){
            					System.out.println("Correcting value for server: "+ser.getCacheServerURL()+" as: "+finalValue);
            					ser.put(key, finalValue);
            				}
            			}
        			}
        			getStatus.clear();
        			return finalValue;
        		} else {
        			System.out.println("Successfully performed get function all servers");
        			getStatus.clear();
        			return getResults.keySet().toArray()[0].toString();
        		}
        	}
        }
		
	}

}

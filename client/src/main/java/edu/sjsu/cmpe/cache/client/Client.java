package edu.sjsu.cmpe.cache.client;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        
        CRDTClient crdt = new CRDTClient();
        crdt.addServer("http://localhost:3000");
        crdt.addServer("http://localhost:3001");
        crdt.addServer("http://localhost:3002");
       
        // 1st put call
        crdt.put(1, "a");
        
        Thread.sleep(30*1000);
        
     /*   //alternate flow by stopping any two server to test put roll back
        crdt.put(1, "b");
        
        Thread.sleep(30*1000);
        
        //turn on all servers again
        crdt.put(1, "a");
        
        Thread.sleep(30*1000);*/
        
        //2nd put call
        //stop any one server to make one server inconsistent for get to correct
        crdt.put(1, "b");
        
        Thread.sleep(30*1000);
        
        //3rd get call
        System.out.println("Value in all server: "+crdt.get(1));
        
        System.out.println("Existing Cache Client...");
    }

}

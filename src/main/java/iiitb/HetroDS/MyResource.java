package iiitb.HetroDS;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import iiitb.HetroDS.database.Database;
/**
 * Root resource (exposed at "myresource" path)
 */
@Path("myresource")
public class MyResource {

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "text/plain" media type.
     *
     * @return String that will be returned as a text/plain response.
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getIt() {
    	
    	/*
    	try{
    		
            // To connect to mongodb server
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
   			
            // Now connect to your databases
            DB db = mongoClient.getDB( "test" );
            System.out.println("Connect to database successfully");
            DBCollection coll = db.getCollection("databasemetadata");
            
            DBCollection collectoion1 = db.createCollection("schema",null);
            DBCursor cursor = coll.find();
            String output = Database.convert(cursor);
            System.out.println(output);
//            boolean auth = db.authenticate(myUserName, myPassword);
//            System.out.println("Authentication: "+auth);
   			
         }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
         }
         */
        return "Got it!";
    }
}

package iiitb.HetroDS.Resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.bson.BSON;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

import com.mongodb.client.model.Filters;

import iiitb.HetroDS.database.Database;
import iiitb.HetroDS.Parser.JsonFlattener;

@Path("HDBOutput")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ManageOutput {
	
	@POST
	@Path("storeFilter/{HDBName}")
	public Response storeFilter(String Data , @PathParam("HDBName") String HDBName)
	{
		System.out.println(Data);
		if(Data.equals("[]"))
			return Response.status(214).build();
		
		JSONObject filterObject = new JSONObject(Data);
		System.out.println("filter Object is : "+filterObject.toString());
		Database.insertDocuments(HDBName, "filter", new JSONArray().put(filterObject));
		return Response.ok().build();
	}
	
	@POST
	@Path("getFilterDetails/{HDBName}/{FilterName}")
	public Response getFilterDetails(@PathParam("HDBName") String HDBName , @PathParam("FilterName") String FilterName)
	{
		System.out.println("Filter Name is :"+FilterName);
		Document where = new Document("filterName",FilterName);
		JSONArray dataToSend = Database.retrieveDocuments(HDBName,"filter",where,null);
		System.out.println("Filter rows :"+dataToSend.toString());
		return Response.ok().entity(dataToSend.get(0).toString()).build();
	}
	
	@POST
	@Path("getAllFilters/{HDBName}")
	public Response getAllFilter(@PathParam("HDBName") String HDBName)
	{
		JSONArray dataToSend = Database.retrieveFilterDocuments(HDBName);
		return Response.ok().entity(dataToSend.toString()).build();
	}
	
	
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("getData/{HDBName}/{FilterName}/{DataFormat}")
	public Response getData(@PathParam("HDBName") String HDBName , @PathParam("FilterName") String FilterName , @PathParam("DataFormat") String DataFormat)
	{

		Document where = new Document("filterName",FilterName);
		JSONArray filterDetails = Database.retrieveDocuments(HDBName, "filter", where, null);
		System.out.println(filterDetails.toString());
		JSONObject filterobject = (JSONObject) filterDetails.get(0);
		System.out.println(filterobject);
		String DataToSend="";
		
		JSONArray RetrievedData = Database.getDataByFilter(HDBName, filterobject);
		
		if(DataFormat.equals("XML")){
			DataToSend = XML.toString(new JSONArray(RetrievedData.toString()));
		}
		else if(DataFormat.equals("JSON")){
			DataToSend = RetrievedData.toString();
		}
		else if(DataFormat.equals("CSV")){
			JsonFlattener parser = new JsonFlattener();
			List<Map<String, String>> flatJson = parser.parse(RetrievedData);
			DataToSend = parser.getCSVString(flatJson);
		}
		System.out.println(DataToSend);
		
		return Response.ok().entity(DataToSend.toString()).build();
	}

}
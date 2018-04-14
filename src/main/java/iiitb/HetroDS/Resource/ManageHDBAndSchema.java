package iiitb.HetroDS.Resource;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

import iiitb.HetroDS.Parser.CSV;
import iiitb.HetroDS.database.Database;

@Path("HDBSchema")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ManageHDBAndSchema {

	@POST
	@Path("createHDB/{HDBName}")
	public Response createHDB(@PathParam("HDBName") String HDBName)
	{
		Database.dbConnect();
		Database.createDB(HDBName);
		System.out.println(Database.getDBNames().toString());
//		System.out.println(HDBName);
		return Response.ok().build();
	}
	
	@POST
	@Path("getAllHDB")
	public Response getAllHDB()
	{
		Database.dbConnect();
		return Response.ok().entity(Database.getDBNames().toString()).build();
	}
	
	@POST
	@Path("insertDataset/{HDBName}/{DatasetName}")
	public Response insertDataset(@PathParam("HDBName") String HDBName, @PathParam("DatasetName") String DatasetName,String data)
	{
		System.out.println("Inside insertDataset");
		Database.dbConnect();
		System.out.println(Database.addCollection(HDBName, DatasetName));
		System.out.println(data);
		JSONObject jobj = new JSONObject(data);
		Database.insertDocuments(HDBName, "schema",new JSONArray().put(jobj));
		return Response.ok().build();
	}
	
	@POST
	@Path("getDatasets/{HDBName}")
	public Response getDatasets(@PathParam("HDBName") String HDBName)
	{
		System.out.println("Inside getDatasets");
		Database.dbConnect();
		return Response.ok().entity(Database.getCollections(HDBName).toString()).build();
	}
	
	@POST
	@Path("updateSchema/{HDBName}")
	public Response updateSchema(@PathParam("HDBName") String HDBName)
	{
		
		return Response.ok().build();
	}
	
	@POST
	@Path("viewDataset/{HDBName}/{DataSetName}")
	public Response viewDataset(@PathParam("HDBName") String HDBName, @PathParam("DataSetName") String DataSetName,String data)	
	{
		System.out.println("Inside viewDataset");
		Database.dbConnect();
		JSONObject attlist = Database.getAttributesObject(HDBName,DataSetName);
		System.out.println("attribute list"+attlist);
		return Response.ok().entity(attlist.get("attributes").toString()).build();
	}
	
	@POST
	@Path("createDatasetFile/{HDBName}/{DataSetName}/{DataFormat}/{Separator}")
	public Response createDataFile(String Data , @PathParam("HDBName") String HDBName, @PathParam("DataSetName") String DataSetName ,@PathParam("DataFormat") String DataFormat,@PathParam("Separator") int Separator) throws IOException
	{
		System.out.println("in createfile dataset");
		JSONArray unfilteredArray = null;
		String[] Attributes = null;
//		If Data is in XML Format
		if(DataFormat.equals("XML")){
			JSONObject xmlJSONObj = null;
			JSONObject xmlJSONObjCaps = null;
			try{
				xmlJSONObj = XML.toJSONObject(Data);
				xmlJSONObjCaps = new JSONObject(xmlJSONObj.toString().toUpperCase());
			}
			catch(Exception e){
				System.out.println(e.getMessage());
				return Response.status(207).build();
			}
			//XML root is not equal to Dataset Name
			if(xmlJSONObjCaps.isNull(DataSetName.toUpperCase())){
				System.out.println("L1");
	        	return Response.status(208).build();
	        }
	        else{
	        	JSONObject innerobj = xmlJSONObj.getJSONObject(DataSetName);
					//More element possible
	        	System.out.println("L2");
					//remove root's all attribute ----------------------------------------------
					//First get index of root element and get index of'>', then remove every attribures.
					StringBuilder stringBuilder = new StringBuilder(Data);
					int startIndexofDataSetName = stringBuilder.indexOf(DataSetName);
					int endIndexofDataSetName = startIndexofDataSetName + DataSetName.length();
					int startindexofEndTag = stringBuilder.indexOf(">",startIndexofDataSetName);
					stringBuilder.replace(endIndexofDataSetName, startindexofEndTag, "");
					String NewData = stringBuilder.toString();
					xmlJSONObj = XML.toJSONObject(NewData);
					innerobj = xmlJSONObj.getJSONObject(DataSetName);
		        	
		        	//Insert only if there are one type of child tags inside the xml root element
		        	if(xmlJSONObj.getJSONObject(DataSetName).length()==1){	
		        		String value = innerobj.get(JSONObject.getNames(innerobj)[0]).toString();
		        		System.out.println("data  "+value);
		        		//If string is a JSONArray
		        		if(value.startsWith("["))
		        		{	      
		        			unfilteredArray = new JSONArray(value);	
		        		}
		        		
		        	}
		        	else{
		        		//ERROR
		        		System.out.println("More than 1 object - ERROR");
		        		return Response.status(209).build();
		        	}
					
				}
	        
		}
		// If Data is in JSON Format
		else if(DataFormat.equals("JSON")){
			try{
				if(Data.trim().startsWith("["))
					unfilteredArray = new JSONArray(Data);
				else if(Data.trim().startsWith("{"))
					unfilteredArray = new JSONArray().put(new JSONObject(Data));
				else
					return Response.status(210).build();
			}
			catch(Exception e){
				return Response.status(210).build();
			}
			
		}
		// If Data is in CSV format
		else if(DataFormat.equals("CSV")){
			try{
				char delimiter;
				if(Separator == 1)
					delimiter = ',';
				else if(Separator == 2)
					delimiter = ';' ;
				else
					delimiter = ':';
				StringReader in = new StringReader(Data);
			    CSV csv = new CSV(true,delimiter, in );
			    List< String > fieldNames = null;
			    if(csv.hasNext()){
			    	fieldNames= new ArrayList< >(csv.next());
			    	Attributes = (String[])fieldNames.toArray();
			    }
			    List< Map <String,Object>> list = new ArrayList< > ();
			    while (csv.hasNext()) {
			        List<String> x = csv.next();
			        Map< String, Object > obj = new LinkedHashMap< > ();
			        for(int i = 0; i < fieldNames.size(); i++) {
			        String fname = fieldNames.get(i);
			        String value = x.get(i);
			        	if(value.matches("([0-9]*[.])?[0-9]+"))
			        	{
			        		if(value.matches("[0-9]+"))
			        			obj.put(fname,Integer.parseInt(value));
			        		else
			        			obj.put(fname,Double.parseDouble(value));
			        	}
			        	else
			        		obj.put(fname,value);
			        		
			        }
			        list.add(obj);
			    }
			    unfilteredArray =  new JSONArray();
			    for( Map < String,Object > obj:list){
			    	unfilteredArray.put(obj);
			    }
			}
			catch(Exception e){
				e.printStackTrace();
				return Response.status(211).build();
			}
		}
		else{
			unfilteredArray = new JSONArray();
		}
		
		JSONObject jobj = new JSONObject();
		jobj.put("schemaName", DataSetName);
		JSONArray attributeArray = new JSONArray();
		System.out.println(unfilteredArray.toString());
		//System.out.println(unfilteredArray.get(0).toString());
		Attributes = JSONObject.getNames(new JSONObject(unfilteredArray.get(0).toString()));
		for(int i=0 ; i < Attributes.length ;i++){
			JSONObject temp = new JSONObject();
			temp.put("attribute_name", Attributes[i]);
	        String value = ((JSONObject)(unfilteredArray.get(0))).get(Attributes[i]).toString();
			if(value.startsWith("{"))
			{
				temp.put("attribute_type","Object");
			}
			else
			{
				if(value.matches("([0-9]*[.])?[0-9]+"))
	        	{
					temp.put("attribute_type","Number");
	        	}
				else
				{
					temp.put("attribute_type", "String");
				}
			}
			
			attributeArray.put(i,temp);
		}
		
		jobj.put("attributes",attributeArray);
		JSONObject example = new JSONObject();
		example.put("example",unfilteredArray);
		jobj.put("sample",example);
		System.out.println("final     \n"+jobj.toString());
		Database.dbConnect();
		Database.addCollection(HDBName, DataSetName);
		Database.insertDocuments(HDBName, "schema",new JSONArray().put(jobj));
		return Response.ok().build();
	}
	
}

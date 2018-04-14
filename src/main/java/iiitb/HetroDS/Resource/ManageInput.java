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
import iiitb.HetroDS.Parser.JsonFlattener;
import iiitb.HetroDS.database.Database;

@Path("HDBInput")
@Consumes(MediaType.APPLICATION_JSON)
public class ManageInput {
	
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("example/{HDBName}/{DataSetName}/{DataFormat}/{Separator}")
	public Response example(@PathParam("HDBName") String HDBName, @PathParam("DataSetName") String DataSetName ,@PathParam("DataFormat") String DataFormat,@PathParam("Separator") int Separator)
	{
		System.out.println("inside example");
		Database.dbConnect();
		JSONObject jso = Database.getAttributesObject(HDBName,DataSetName);
		System.out.println(jso);
		if(DataFormat.equals("JSON"))
		{
			JSONObject	temp = (new JSONObject(jso.get("sample").toString()));
			return Response.ok().entity(temp.get("example").toString()).build();
		}
			
		else if(DataFormat.equals("XML"))
		{
			String xml = "<"+DataSetName+">"+ XML.toString(jso.get("sample")) + "</"+DataSetName+">";
			
			return Response.ok().entity(xml).build();
		}
		else
		{
			char delimiter;
			if(Separator == 1)
				delimiter = ',';
			else if(Separator == 2)
				delimiter = ';' ;
			else
				delimiter = ':';
			JSONObject	temp = (new JSONObject(jso.get("sample").toString()));
			JSONArray jsoarray = new JSONArray(temp.get("example").toString());
			String csv = null;
	//		csv = CDL.toString(jsoarray);
	//		csv = csv.replace(',',delimiter);
			
			JsonFlattener parser = new JsonFlattener();
			List<Map<String, String>> flatJson = parser.parse(jsoarray);
			csv = parser.getCSVString(flatJson,delimiter);
			
			return Response.ok().entity(csv).build();
		}
			
	}
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("insertData/{HDBName}/{DataSetName}/{DataFormat}/{Separator}")
	public Response insertData(String Data , @PathParam("HDBName") String HDBName, @PathParam("DataSetName") String DataSetName ,@PathParam("DataFormat") String DataFormat,@PathParam("Separator") int Separator) throws IOException
	{
		JSONArray unfilteredArray = null;
//		If Data is in XML Format
		if(DataFormat.equals("XML")){
			JSONObject xmlJSONObj = null;
			JSONObject xmlJSONObjCaps = null;
			try{
				xmlJSONObj = XML.toJSONObject(Data);
				xmlJSONObjCaps = new JSONObject(xmlJSONObj.toString().toUpperCase());
			}
			catch(Exception e){
				return Response.status(207).build();
			}
			//XML root is not equal to Dataset Name
			if(xmlJSONObjCaps.isNull(DataSetName.toUpperCase())){
//	        	System.out.println("Error..!!");
	        	return Response.status(208).build();
	        }
	        else{
	        	JSONObject innerobj = xmlJSONObj.getJSONObject(DataSetName);
//				System.out.println(innerobj.toString());
				
				//Get all column from schema   ------------------------------------------------
				ArrayList<String> columnlist = Database.getAttributeNames(HDBName, DataSetName);
				//ColumnList Added
				//Check and compare all columns with given root's attribute or element
				//If anyone matches then we have only one element(row)
				boolean oneelment = false;
				for(String column:columnlist){
					if(!innerobj.isNull(column)){
						oneelment = true;
						break;
					}
				}
				if(oneelment){
					//Only one element -- insert to database
					unfilteredArray = new JSONArray().put(innerobj);
					System.out.println("INSERTED DATA - "+unfilteredArray.toString());
					//return Response.status(201).build();
				}
				else{
					//More element possible
					
					//remove root's all attribute ----------------------------------------------
					//First get index of root element and get index of'>', then remove every attribures.
					StringBuilder stringBuilder = new StringBuilder(Data);
					int startIndexofDataSetName = stringBuilder.indexOf(DataSetName);
					int endIndexofDataSetName = startIndexofDataSetName + DataSetName.length();
					int startindexofEndTag = stringBuilder.indexOf(">",startIndexofDataSetName);
					stringBuilder.replace(endIndexofDataSetName, startindexofEndTag, "");
//					System.out.println(stringBuilder.toString());
					
					String NewData = stringBuilder.toString();
					xmlJSONObj = XML.toJSONObject(NewData);
					innerobj = xmlJSONObj.getJSONObject(DataSetName);
		        	
		        	//Insert only if there are one type of child tags inside the xml root element
		        	if(xmlJSONObj.getJSONObject(DataSetName).length()==1){	
		        		String value = innerobj.get(JSONObject.getNames(innerobj)[0]).toString();
		        		
		        		//If string is a JSONArray
		        		if(value.startsWith("[")){	      
		        			unfilteredArray = new JSONArray(value);
		        		}
		        		//If string is a JSONObject
		        		else{
		        			unfilteredArray = new JSONArray().put(new JSONObject(value));
		        		}
		        		System.out.println("INSERTED DATA - "+unfilteredArray);
		        		
		        	}
		        	else{
		        		//ERROR
		        		System.out.println("More than 1 object - ERROR");
		        		return Response.status(209).build();
		        	}
					
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
//			Database.insertDocuments(HDBName,DataSetName,jArray);
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
//		    System.out.println(unfilteredArray);
//    		Database.insertDocuments(HDBName,DataSetName,jsonarray);
		}
		else{
			unfilteredArray = new JSONArray();
		}
		JSONObject jobj = Database.filterJSONArray(Database.getAttributeNameValues(HDBName,DataSetName),unfilteredArray);
		JSONArray filteredArray = jobj.getJSONArray("tuples");
		jobj.remove("tuples");
		if(filteredArray.length()>0)
			Database.insertDocuments(HDBName,DataSetName,filteredArray);
		System.out.println("Sending... "+jobj.toString());
		return Response.ok().entity(jobj.toString()).build();
	}
}
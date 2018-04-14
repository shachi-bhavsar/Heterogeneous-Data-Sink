package iiitb.HetroDS.database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

public class Database {
	
	public static MongoClient mongoClient=null;
	
	static boolean isNumeric(String str)
	{
	  return str.matches("-?\\d+(\\.\\d+)?");  //match a number with optional '-' and decimal.
	}
	
	public static void dbConnect() throws MongoException {
		if(mongoClient == null)
			mongoClient = new MongoClient( "localhost" , 27017 );	
	}
	
	public static JSONArray getDBNames() throws MongoException {
		dbConnect();
		MongoIterable<String> dblist = mongoClient.listDatabaseNames();
		JSONArray dbarray = new JSONArray();
		for(String dbname:dblist)
			dbarray.put(dbname);
		return dbarray;
		
	}
	
	public static MongoDatabase useDB(String dbName) throws MongoException {
		dbConnect();
		MongoDatabase db = mongoClient.getDatabase( dbName );
        return db;
	}
	
	public static MongoDatabase createDB(String dbName) throws MongoException {
		MongoDatabase db = useDB(dbName);
		db.createCollection("schema");
        return db;
	}
	
	public static JSONArray getCollections(String dbName) throws MongoException {
		JSONArray collArray = new JSONArray();
		MongoDatabase db = useDB(dbName);
		MongoIterable<String> collList = db.listCollectionNames();
		for(String collectionName:collList)
			collArray.put(collectionName);
		return collArray;
	}
	
	public static boolean addCollection(String dbName,String collectionName) throws MongoException {
		MongoDatabase db = useDB(dbName);
		db.createCollection(collectionName);
		MongoIterable<String> collList = db.listCollectionNames();
		for(String collection:collList)
			if(collection.equals(collectionName))
				return true;
		return false;
	}
	
	public static Document generateWhereQuery(JSONArray whereContent){
		System.out.println("generating query for: "+whereContent.toString());
		
//		If no whereContent, or no clauses in whereContent, then find all documents in the collection		
		if(whereContent == null || whereContent.length()==0)
			return new Document();
		if(whereContent.length()<=2){
			String connector = ((JSONObject)whereContent.get(0)).getString("Connector");
			
			Document where = new Document();
			List<Document> clauses = new ArrayList<Document>();
			JSONObject clause;
			
	//		For every clause in whereContent		
			for(int i=0;i<whereContent.length();i++){
				clause = whereContent.getJSONObject(i);
				
	//			If value is Numeric			
				if (isNumeric(clause.getString("Value"))){
					clauses.add(new Document(clause.getString("Attribute"),
						new Document(clause.getString("Operator"),clause.getInt("Value"))));
				}
	//			Value is string			
				else{
					clauses.add(new Document(clause.getString("Attribute"),
							new Document(clause.getString("Operator"),clause.getString("Value"))));
				}
			}
	//		
			if(connector.equals("AND"))
				where.append("$and", clauses);
			else
				where.append("$or", clauses);
			System.out.println("returning..."+where.toString());
			return where;
		}
		else{
			
			Document where = new Document();
			Document firstclause;
			Document secondclause;
			
			List<Document> templist = new ArrayList<Document>();
			
			JSONObject clause;
			clause = whereContent.getJSONObject(0);
			if (isNumeric(clause.getString("Value"))){
				firstclause = new Document(clause.getString("Attribute"),
					new Document(clause.getString("Operator"),clause.getInt("Value")));
			}
			else{
				firstclause = new Document(clause.getString("Attribute"),
						new Document(clause.getString("Operator"),clause.getString("Value")));
			}
			String curconnector = clause.getString("Connector");
			
			for(int i=1;i<whereContent.length();i++){
				templist.clear();
				clause = whereContent.getJSONObject(i);
				if (isNumeric(clause.getString("Value"))){
					secondclause = new Document(clause.getString("Attribute"),
						new Document(clause.getString("Operator"),clause.getInt("Value")));
				}
				else{
					secondclause = new Document(clause.getString("Attribute"),
							new Document(clause.getString("Operator"),clause.getString("Value")));
				}
				templist.add(new Document().parse(firstclause.toJson()));
				templist.add(new Document().parse(secondclause.toJson()));
				where.clear();
				if(curconnector.equals("AND"))
					where.append("$and", templist);
				else
					where.append("$or", templist);
				curconnector = clause.getString("Connector");
				firstclause = new Document().parse(where.toJson());
			}
			System.out.println("returning..."+where.toString());
			return where;
		}
	}
	
	public static JSONArray getDataByFilter(String dbName,JSONObject filterObject){
		
		MongoDatabase db = useDB(dbName);
		MongoCollection<Document> filterCollection = db.getCollection("filter");
	
		JSONArray whereContent = filterObject.getJSONArray("whereContent");
		JSONArray projectContent = filterObject.getJSONArray("projectContent");
		ArrayList<String> projectKeys = new ArrayList<String>();
		for(Object obj:projectContent)
			projectKeys.add((String)obj);
		Document where = generateWhereQuery(whereContent);
		return retrieveDocuments(dbName, filterObject.getString("datasetName"), where, projectKeys);
	}
	
	public static boolean insertDocuments(String dbName, String collName,JSONArray tuples){
	
		ArrayList<Document> documentsList = new ArrayList<Document>();
		for(int index = 0;index<tuples.length();index++){
			documentsList.add(Document.parse( tuples.getJSONObject(index).toString()));
		}
		MongoDatabase db = useDB(dbName);
		MongoCollection<Document> coll = db.getCollection(collName);
		coll.insertMany(documentsList);
		return true;
	}
	
	public static JSONObject getAttributesObject(String dbName,String datasetName){
//		Returns schema details for given dataset
		MongoDatabase db = useDB(dbName);
		MongoCollection<Document> coll = db.getCollection("schema");
		Bson filter = Filters.eq("schemaName",datasetName);
		Document Doc = coll.find(filter).projection(Projections.excludeId()).first();
		return new JSONObject(Doc.toJson());
	}
	
	public static ArrayList<String> getAttributeNames(String dbName,String datasetName){
// 		Returns an arraylist of attribute names for given dataset	
		
		JSONObject attributesObject = getAttributesObject(dbName,datasetName);
		JSONArray attributesArray = attributesObject.getJSONArray("attributes");
		ArrayList<String> attributes = new ArrayList<String>();
		for(int index = 0;index<attributesArray.length();index++){
			JSONObject jObj = (JSONObject)attributesArray.get(index);
			attributes.add(jObj.getString("attribute_name"));
		}
		return attributes;	
	}
	
	public static HashMap<String,String> getAttributeNameValues(String dbName,String datasetName){
// 		Returns an arraylist of attribute names for given dataset	
		
		JSONObject attributesObject = getAttributesObject(dbName,datasetName);
		JSONArray attributesArray = attributesObject.getJSONArray("attributes");
		HashMap<String,String> attributes = new HashMap<String,String>();
		for(int index = 0;index<attributesArray.length();index++){
			JSONObject jObj = (JSONObject)attributesArray.get(index);
			attributes.put(jObj.getString("attribute_name"),jObj.getString("attribute_type"));
		}
		return attributes;	
	}
	
	public static JSONArray retrieveDocuments(String dbName , String collName , Document where , ArrayList<String> projectKeys){
		
		dbConnect();
		MongoDatabase db = mongoClient.getDatabase(dbName);
		MongoCollection<Document> coll = db.getCollection(collName);
		FindIterable<Document> documents;
		if(projectKeys != null && projectKeys.size()>0)
			documents = coll.find(where).projection(Projections.fields(Projections.include(projectKeys),Projections.excludeId()));
		else
			documents = coll.find(where).projection(Projections.excludeId());
		JSONArray retrievedtuples = new JSONArray();
		for(Document doc:documents)
			retrievedtuples.put(new JSONObject(doc.toJson()));
		return retrievedtuples;
	}
	
public static JSONObject filterJSONArray(HashMap<String,String> datasetKeys,JSONArray tuples){
		
		JSONObject oldObj,newObj,ignoredKeys = new JSONObject();
		int failCount = 0;
		boolean mismatch=true;
		Set<String> ks = datasetKeys.keySet();
		
//		Create a HashMap of required keys		
		JSONObject keyMap = new JSONObject();
		for(String key:ks){
			keyMap.put(key, 1);
		}
		
		JSONArray filteredTuples = new JSONArray();
//		For every row/tuple in input JSONArray		
		for(Object tuple:tuples){
			oldObj = (JSONObject)tuple;
			newObj = new JSONObject();
			mismatch=false;
//			Copy the required key to new JSONObject			
			for(String key:ks){
				if(!oldObj.has(key))
					break;
	
				newObj.put(key, oldObj.get(key));
			}
//			all Required keys filtered into new object			
			if(datasetKeys.size() == newObj.keySet().size()){
//				Put the new Object in filtered tuples array	
				for(String nkey:JSONObject.getNames(newObj)){
					System.out.println("testing for "+newObj.toString());
					if(datasetKeys.get(nkey).equals("Number") && !isNumeric(newObj.get(nkey).toString())){
						failCount++;
						mismatch=true;
						System.out.println("mismatch for "+nkey+" value: "+newObj.getString(nkey));
						break;
					}
				}
				if(!mismatch)	
					filteredTuples.put(newObj);
//				Find all ignored keys and add if key not already present 				
				for(Object key:oldObj.keySet().toArray()){
					String sKey = (String)key;
//					If key is not a required key, and has been encountered first time					
					if(keyMap.isNull(sKey) && ignoredKeys.isNull(sKey)){
						ignoredKeys.put(sKey, true);
					}
				}
			}
			else{
//				Increment failed tuples count 				
				failCount++;
			}
		}
		String[] ignoredNames = JSONObject.getNames(ignoredKeys);
		if(ignoredNames == null){
			ignoredNames = new String[1];
			ignoredNames[0] = "None";
		}
		return new JSONObject().put("tuples",filteredTuples)
				.put("ignoredKeys", ignoredNames).put("failedCount", failCount).put("insertCount",filteredTuples.length());
	}

	//To get list of filter for specific database
	public static JSONArray retrieveFilterDocuments(String dbName) {
		
		dbConnect();
		MongoDatabase db = mongoClient.getDatabase(dbName);
		MongoCollection<Document> coll = db.getCollection("filter");
		FindIterable<Document> documents = coll.find().projection(Projections.fields(Projections.include("filterName"),Projections.excludeId()));
		JSONArray retrievedtuples = new JSONArray();
		for(Document doc:documents)
			retrievedtuples.put(new JSONObject(doc.toJson()));
		return retrievedtuples;

	}	
}

package iiitb.HetroDS.Parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;



public class JsonFlattener {
    public Map<String, String> parse(JSONObject jsonObject) {
        Map<String, String> flatJson = new HashMap<String, String>();
        flatten(jsonObject, flatJson, "");
        return flatJson;
    }

    public List<Map<String, String>> parse(JSONArray jsonArray) {
        List<Map<String, String>> flatJson = new ArrayList<Map<String, String>>();
        int length = jsonArray.length();
        for (int i = 0; i < length; i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            Map<String, String> stringMap = parse(jsonObject);
            flatJson.add(stringMap);
        }
        return flatJson;
    }

    public List<Map<String, String>> parseJson(String json) throws Exception {
        List<Map<String, String>> flatJson = null;
        try {
            JSONObject jsonObject = new JSONObject(json);
            flatJson = new ArrayList<Map<String, String>>();
            flatJson.add(parse(jsonObject));
        } catch (JSONException je) {
            flatJson = handleAsArray(json);
        }
        return flatJson;
    }

    private List<Map<String, String>> handleAsArray(String json) throws Exception {
        List<Map<String, String>> flatJson = null;
        try {
            JSONArray jsonArray = new JSONArray(json);
            flatJson = parse(jsonArray);
        } catch (Exception e) {
            throw new Exception("Json might be malformed");
        }
        return flatJson;
    }

    private void flatten(JSONArray obj, Map<String, String> flatJson, String prefix) {
        int length = obj.length();
        for (int i = 0; i < length; i++) {
            if (obj.get(i).getClass() == JSONArray.class) {
                JSONArray jsonArray = (JSONArray) obj.get(i);
                if (jsonArray.length() < 1) continue;
                flatten(jsonArray, flatJson, prefix + i);
            } else if (obj.get(i).getClass() == JSONObject.class) {
                JSONObject jsonObject = (JSONObject) obj.get(i);
                flatten(jsonObject, flatJson, prefix + (i + 1));
            } else {
                String value = String.valueOf(obj.get(i));
                if (value != null)
                    flatJson.put(prefix + (i + 1), value);
            }
        }
    }

    private void flatten(JSONObject obj, Map<String, String> flatJson, String prefix) {
        Iterator iterator = obj.keys();
        while (iterator.hasNext()) {
            String key = iterator.next().toString();
            if (obj.get(key).getClass() == JSONObject.class) {
                JSONObject jsonObject = (JSONObject) obj.get(key);
                flatten(jsonObject, flatJson, prefix);
            } else if (obj.get(key).getClass() == JSONArray.class) {
                JSONArray jsonArray = (JSONArray) obj.get(key);
                if (jsonArray.length() < 1) continue;
                flatten(jsonArray, flatJson, key);
            } else {
                String value = String.valueOf(obj.get(key));
                if (value != null && !value.equals("null"))
                    flatJson.put(prefix + key, value);
            }
        }
    }
    
    public String getCSVString(List<Map<String, String>> flatJson,char delimiter)
    {
    	Set<String> headers = collectHeaders(flatJson);
        String output = StringUtils.join(headers.toArray(), delimiter) + "\n";
        for (Map<String, String> map : flatJson) {
            output = output + getCommaSeperatedRow(headers, map, delimiter) + "\n";
        }
        return output;
    }
    
    public String getCSVString(List<Map<String, String>> flatJson)
    {
    	return getCSVString(flatJson,',');
    }
    
    private String getCommaSeperatedRow(Set<String> headers, Map<String, String> map, char delimiter) {
        List<String> items = new ArrayList<String>();
        for (String header : headers) {
            String value = map.get(header) == null ? "" : map.get(header).replace(",", "");
            items.add(value);
        }
        return StringUtils.join(items.toArray(),delimiter);
    }

    private Set<String> collectHeaders(List<Map<String, String>> flatJson) {
        Set<String> headers = new TreeSet<String>();
        for (Map<String, String> map : flatJson) {
            headers.addAll(map.keySet());
        }
        return headers;
    }
}


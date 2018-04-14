var inside_data=[];
var attribute_data={
    schemaName : "",
    attributes: [] ,
    sample : ""
};
var restrictedKeywords = ["schema","filter","system.indexes"];
var filedata;
function doOnLoad()
{
        $("#schema").load("Schema.html");
        $("#input").load("Input.html");
        $("#output").load("Output.html");
        getAllHDB();
        getAllInputHDB();
        getAllOutputHDB();
}
//Addition on 20th April
function getSampleFileData(event)
{
    //alert('in file');
    for (var i = 0; i < event.target.files.length; ++i) {
    (function (file) {               // Wrap current file in a closure.
      var loader = new FileReader();
      loader.onload = function (loadEvent) {
        if (loadEvent.target.readyState != 2)
          return;
        if (loadEvent.target.error) {
          alert("Error while reading file " + file.name + ": " + loadEvent.target.error);
          return;
        }
        console.log(loadEvent.target.result.length); // Your text is in loadEvent.target.result
        filedata = loadEvent.target.result;
        //alert(filedata);
      };
      loader.readAsText(file);
    })(event.target.files[i]);
  }
   
}

function displaySeparatorForSchema()
{
    //alert('inside fillExample');
    var csv = $('#format_select').val();
    if(csv == 'CSV')
        document.getElementById("div2").style.display = "";
    else
        document.getElementById("div2").style.display = "none";

}
function createDatasetFromFile()
{
    dbname = $('#db_select').val();
    datasetname = $('#datasetname').val();
    var dataformat = $('#format_select').val();
    var separator = 2;


    if(dataformat == 'CSV')
    {
        separator = $('#separator').val();
    }
     //alert("in create from file");
	  $.ajax({
	        url: 'http://localhost:8080/HetroDS/webapi/HDBSchema/createDatasetFile/'+dbname+'/'+datasetname+'/'+dataformat+'/'+separator,
	        type: 'POST',
	        async:false,
	        contentType : 'application/json',
	        dataType : 'json',
	        data: filedata,
	        complete : function(request, textStatus, errorThrown) {
	            
	            if(request.status == 200)
	   	    		 alert("Schema Created");
            	if(request.status == 207)
	   	    		 alert("Data Not Inserted..!! Invalid XML format");
	   	    	if(request.status == 208)
	   	    		 alert("Data Not Inserted..!! Root tag does not match with Dataset Name");
	   	    	if(request.status == 209)
	   	    		 alert("Data Not Inserted..!! More than one type of object inside XML");
	   	    	if(request.status == 210)
	   	    		 alert("Data Not Inserted..!! Invalid JSON format");
	   	    	if(request.status == 211)
	   	    		 alert("Data Not Inserted..!! Invalid CSV format");
	   	    	if(request.status == 500)
	   	    		alert("Input format is not selected");
	        }
	    });
       
        jQuery.noConflict();
        $('#create_dataset_modal').modal('toggle');
        $('#create_datasetFromFile_modal').modal('toggle');
        $('.modal').find("input,textarea").val('');
        getAllDatasetsForHDB('#db_select','#schema_select');

}

function clearDiv()
{
    document.getElementById("addedAttributes").innerHTML = "";
    document.getElementById("viewAtributes_div").style.display = "none";
    attribute_data.attributes.length = 0 ;
    inside_data.length = 0;
}

function viewDataset()
{
    dbname = $('#db_select').val();
    datasetname = $('#schema_select').val();
    document.getElementById("viewDataset_div").style.display = "";
    $('#dataset_table').empty();
    $('#dataset_heading').empty();
    if(datasetname != "Select Dataset")
    {
        $.ajax({
            url: 'http://localhost:8080/HetroDS/webapi/HDBSchema/viewDataset/'+dbname+'/'+datasetname,
            type: 'POST',
            async:false,
            contentType : 'application/json',
            dataType : 'json',
            success : function(data){
            //	$('#dataset_caption').append('<b>'+datasetname+'</b>');
            	$('#dataset_heading').append('<tr>'+
        		        '<th>Attribute name</th>'+
        		        '<th>DataType</th>'+
        		      +'</tr>');
                for(var i=0 ; i < data.length ; i++){
                    $('#dataset_table').append('<tr>'+
                        '<td>'+data[i]['attribute_name']+'</td>'+
                        '<td>'+data[i]['attribute_type']+'</td>'+
                        '</tr>'
                        );
                }
            },
            complete : function(request, textStatus, errorThrown) {
                //alert("Schema created");
            }
            
        });
    }

}
function createHDB()
{
	dbname = $('#dbname').val();
	if(dbname == "")
		return;
	else if($("#db_select option[value='"+dbname+"']").length > 0)
	{
		alert("Database already exists with same name!!");
		jQuery.noConflict();
     //   $('#create_db_modal').modal('toggle');
        $('.modal').find("input,textarea").val('');
        attribute_data.attributes.length = 0 ;
        inside_data.length = 0;
		return;
		
	}
   // document.getElementById("viewDataset_div").style.display = "none"; // this is to hide the dataset view 
	$.ajax({
        url: 'http://localhost:8080/HetroDS/webapi/HDBSchema/createHDB/'+dbname,
        type: 'POST',
        async:false,
        contentType : 'application/json',
        dataType : 'json',
        complete : function(request, textStatus, errorThrown) {
        	//alert("HDB created");
		}
	});
}


function getAllHDB()
{
	$('#db_select').empty();
	$('#db_select').append('<option hidden="true">Select DB</option>');
	$.ajax({
        url: 'http://localhost:8080/HetroDS/webapi/HDBSchema/getAllHDB',
        type: 'POST',
        async:false,
        contentType : 'application/json',
        dataType : 'json',
        success : function(data){
//        	alert(data.length);
        	for(var i=0 ; i<data.length ; i++){
        		$('#db_select').append('<option value="'+data[i]+'">'+data[i]+'</option>');
        	}
        },
        complete : function(request, textStatus, errorThrown) {
		}
	});
}
function dataSetExist()
{
    datasetname = $('#datasetname').val();
    if($("#schema_select option[value='"+datasetname+"']").length > 0)
    {
        alert("Dataset already exists with same name!!");
        jQuery.noConflict();
       // $('#create_dataset_modal').modal('toggle');
        // $('#insert_attribute_modal').modal('toggle');
        // $('#create_datasetFromFile_modal').modal('toggle');
        $('.modal').find("input,textarea").val('');
        attribute_data.attributes.length = 0 ;
        inside_data.length = 0;
        document.getElementById("viewAtributes_div").style.display = "none";
        return;
        
    }
    else
    {
         jQuery.noConflict();
         $('#create_datasetFromFile_modal').modal('toggle');
    }

}

function checkDBSelect()
{
	dbname = $('#db_select').val();
	if(dbname=="--Database--")
	{
		alert("Select Database");
		jQuery.noConflict();
		return;
	}
	else
	{
		jQuery.noConflict();
		$('#create_dataset_modal').modal('toggle');
	}
}

function createDataset()
{   
    document.getElementById("viewDataset_div").style.display = "none"; // this is to hide the dataset view 
	dbname = $('#db_select').val();
	//alert(dbname);
	
	datasetname = $('#datasetname').val();
	if(datasetname == "" || $.inArray(datasetname,restrictedKeywords) != -1  ){
		alert("Invalid Dataset Name");
		jQuery.noConflict();
        $('#create_dataset_modal').modal('toggle');
        $('#insert_attribute_modal').modal('toggle');
        $('#create_datasetFromFile_modal').modal('toggle');
        $('.modal').find("input,textarea").val('');
        attribute_data.attributes.length = 0 ;
        inside_data.length = 0;
        document.getElementById("viewAtributes_div").style.display = "none";
		return;
	}
	
	
	attribute_data.schemaName = datasetname;
    sample_data = '{"example":[{';
    for(var i=0 ; i < inside_data.length ; )
    {
        sample_data = sample_data + '"'+inside_data[i++] + '":' ;
        if(inside_data[i] == 'String')  
            sample_data = sample_data +  '"sample"';
        else if(inside_data[i] == 'Number')
            sample_data = sample_data +  '101';
        else
        	sample_data = sample_data + '{"InnerObjects":"samples1"}';
        i++;
        if(i < inside_data.length ) sample_data = sample_data + ',';
        else sample_data = sample_data + '},{';
    }

    for(var i=0 ; i < inside_data.length ; )
    {
        sample_data = sample_data + '"'+inside_data[i++] + '":' ;
        if(inside_data[i] == 'String')  
            sample_data = sample_data +  '"sample2"';
        else if(inside_data[i] == 'Number')
            sample_data = sample_data +  '102';
        else
        	sample_data = sample_data + '{"InnerObjects":"samples2"}';
        i++;
        if(i < inside_data.length ) sample_data = sample_data + ',';
        else sample_data = sample_data + '}]}';
    }
    //alert(sample_data);
    attribute_data.sample = JSON.parse(sample_data);
    //alert(attribute_data);
	$.ajax({
        url: 'http://localhost:8080/HetroDS/webapi/HDBSchema/insertDataset/'+dbname+'/'+datasetname,
        type: 'POST',
        async:false,
        contentType : 'application/json',
        dataType : 'json',
        data: JSON.stringify(attribute_data),
        complete : function(request, textStatus, errorThrown) {
        	//alert("Schema created");
		}
	});
        //alert('sdnfjdfnskdj');
        jQuery.noConflict();
        $('#create_dataset_modal').modal('toggle');
        $('#insert_attribute_modal').modal('toggle');
        $('#create_datasetFromFile_modal').modal('toggle');
        $('.modal').find("input,textarea,select").val('');
        attribute_data.attributes.length = 0 ;
        inside_data.length = 0;
        getAllDatasetsForHDB('#db_select','#schema_select');
        $('#addedAttributes').empty();
        document.getElementById("viewAtributes_div").style.display = "none";
        viewDataset();
}
	

function getAllDatasetsForHDB(get_id,put_id)
 {
//	alert("msg: "+get_id+" "+put_id);
//    document.getElementById("viewDataset_div").style.display = "none";  // this is to hide the dataset view
	dbname = $(get_id).val();
	$(put_id).empty();
	$(put_id).append('<option value="Select Dataset">Select Dataset</option>');
//	$('#schema_select').append('<option hidden="true">Select Dataset</option>');
	$.ajax({
        url: 'http://localhost:8080/HetroDS/webapi/HDBSchema/getDatasets/'+dbname,
        type: 'POST',
        async:false,
        contentType : 'application/json',
        dataType : 'json',
        success : function(data){
//        	alert(data);
        	
        	for(var i=0 ; i<data.length ; i++){
        		if($.inArray(data[i],restrictedKeywords) == -1)
        			$(put_id).append('<option value="'+data[i]+'">'+data[i]+'</option>');
        	}
        	
        },
        complete : function(request, textStatus, errorThrown) {
        	
		}
	});
	
}

// TO generate json of attributes 

function addAttribute()
{
	var attname = $('#attributename').val();
    if(attname == "")
    	return;
    
	$('#addedAttributes').empty();
    attribute_data.attributes.push({ 
        "attribute_name":$('#attributename').val(),
        "attribute_type":$('#attributeType').val(),
    });
        
        var datatype = $('#attributeType').val();
        inside_data.push(attname,datatype);
      $('#attributename').val("");   
      
      document.getElementById("viewAtributes_div").style.display = "";
      
      for(var i=0 ; i<attribute_data.attributes.length ; i++){
    	  $('#addedAttributes').append(
    		  '<tr>'+	  
              '<td>'+attribute_data.attributes[i]['attribute_name']+'</td>'+
              '<td>'+attribute_data.attributes[i]['attribute_type']+'</td>'+
              '</tr>'
           );
      }
      
}


function onKeyPressEventValidation(event,get_id,next_link_id)
{
//	alert("Key pressed");
	var retrived_data = $(get_id).val();
//	alert(retrived_data);
	if(retrived_data != "" && $.inArray(retrived_data,restrictedKeywords) == -1){
		//disable link
		document.getElementById(next_link_id).style.pointerEvents = "all";
	}
	else{
		//enable link
		document.getElementById(next_link_id).style.pointerEvents = "none";
	}
}
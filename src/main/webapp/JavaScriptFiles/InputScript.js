var filedata;

function displaySeparator()
{
	//alert('inside fillExample');
	var csv = $('#input_select').val();
	if(csv == 'CSV')
		document.getElementById("div1").style.display = "";
	else
		document.getElementById("div1").style.display = "none";

}

function checkDbSelect()
{
	dbname = $('#Inputdb_select').val();
	var datatype = $('#input_select').val();
	//alert(dbname);
	if(dbname=="--Database--")
	{
		alert("Select Database");
		jQuery.noConflict();
		if(datatype=="--Input Format--")
		{
			alert("Select Input format");
			jQuery.noConflict();
		}
		return;
	}
	else if(datatype=="--Input Format--")
	{
		alert("Select Input format");
		jQuery.noConflict();
		return;
	}
	else
	{
		jQuery.noConflict();
		$('#create_input_modal').modal('toggle');
	}
}

function example()
{
	var dbname = $('#Inputdb_select').val();
	var datasetname = $('#Inputschema_select').val();
	var separator = 2;
	var datatype = $('#input_select').val();
	//alert(datasetname);
	if(datasetname == "Select Dataset")
	{
		alert("Select Dataset!!");
		jQuery.noConflict();
		return;
	}
	else
	{
		jQuery.noConflict();
		$('#insert_input_file_modal').modal('toggle');
		if(datatype == 'CSV')
		{
			separator = $('#separator_select').val();
		}
		$.ajax({
		    url: 'http://localhost:8080/HetroDS/webapi/HDBInput/example/'+dbname+'/'+datasetname+'/'+datatype+'/'+separator,
		    type: 'POST',
		    async:false,
		    contentType : 'application/json',
		    dataType : 'text',
		    success :function(data){
		    	$('#insert_input_file_modal #inputName').val(data);
		    },
		    complete : function(request, textStatus, errorThrown) {
		    	
			}
		});
	}	
}
function getfiledata(event)
{
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
        $('#insert_input_file_modal #inputName').val(filedata);
      };
      loader.readAsText(file);
    })(event.target.files[i]);
  }
}

function insertData()
{
	var dbname = $('#Inputdb_select').val();
	var datasetname = $('#Inputschema_select').val();
	var separator = 2;
	var datatype = $('#input_select').val();

	if(datatype == 'CSV')
	{
		separator = $('#separator_select').val();
	}
	if($('#inputName').val()){
		//Data is in textarea
		filedata = $('#inputName').val()//get data from text area
	}
	else{
//		alert("From file");
	}
	
	$.ajax({
	    url: 'http://localhost:8080/HetroDS/webapi/HDBInput/insertData/'+dbname+'/'+datasetname+'/'+datatype+'/'+separator,
	    type: 'POST',
	    async:false,
	    contentType : 'application/json',
	    data : filedata,
	    dataType : 'json',
	    success : function(data){
	    	alert(data['insertCount'] +' rows inserted\n'+'failed count = '+data['failedCount']+'       ignored keys: '+data['ignoredKeys']);
	    },
	    complete : function(request, textStatus, errorThrown) {
	    	
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

		}
	});
  jQuery.noConflict();
  $('#create_input_modal').modal('toggle');
  $('#insert_input_file_modal').modal('toggle');
  $('.modal').find("input,textarea,select").val('');
	
}


function getAllInputHDB()
{
	$('#Inputdb_select').empty();
	$('#Inputdb_select').append('<option hidden="true">--Database--</option>');
	$.ajax({
        url: 'http://localhost:8080/HetroDS/webapi/HDBSchema/getAllHDB',
        type: 'POST',
        async:false,
        contentType : 'application/json',
        dataType : 'json',
        success : function(data){
        	for(var i=0 ; i<data.length ; i++){
        		$('#Inputdb_select').append('<option value="'+data[i]+'">'+data[i]+'</option>');
        	}	
        },
        complete : function(request, textStatus, errorThrown) {
		}
	});
}

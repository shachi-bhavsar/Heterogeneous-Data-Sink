var filter_data = {
    filterName : "",
    datasetName : "",
    whereContent: [],
    projectContent: []
};

var dbname;
var filter_Name;
var datasetname;
var attributeName;
var op;
var val;
var connector;

function clearAll(){
	
	//made by shachi
	dbname = $('#Outputdb_select').val();
	if(dbname=="--Database--")
	{
		alert("Select Database");
		jQuery.noConflict();
		return;
	}
	else
	{
		//ends
		document.getElementById("addFilterAttributes").innerHTML = "";
	    document.getElementById("viewFilter_div").style.display = "none";
	    $('.modal').find("input,textarea").val('');
	    $('#output_attribute_modal #hierarchyValue').val('');
	    filter_data.filterName = "";   
	    filter_data.projectContent.length = 0;
	    filter_data.whereContent.length = 0;
	    jQuery.noConflict();
	    $('#create_output_modal').modal('toggle');
	}
}

function checkDSselect()
{
	datasetname = $('#Outputschema_select').val();
	//alert(datasetname);
	if(datasetname=="Select Dataset")
	{
		alert("Select Dataset");
		jQuery.noConflict();
		return;
	}
	else
	{
		jQuery.noConflict();
		$('#create_filter_modal').modal('toggle');
		$('#create_output_modal').modal('hide');
	}
}

function checkFilterSelect()
{
	dbname = $('#Outputdb_select').val();
	filterName = $('#filter_select').val();
	//alert(filterName);
	if(dbname=="--Database--")
	{
		alert("Select Database");
		jQuery.noConflict();
		return;
	}
	if(filterName=="Select Filter")
	{
		alert("Select Filter");
		jQuery.noConflict();
		return;
	}
	else
	{
		jQuery.noConflict();
		$('#select_output_format_modal').modal('toggle');
	}
}

function getAllOutputHDB()
{
    $('#Outputdb_select').empty();
    $('#Outputdb_select').append('<option hidden="true">--Database--</option>');
    $.ajax({
        url: 'http://localhost:8080/HetroDS/webapi/HDBSchema/getAllHDB',
        type: 'POST',
        async:false,
        contentType : 'application/json',
        dataType : 'json',
        success : function(data){
//          alert(data.length);
            for(var i=0 ; i<data.length ; i++){
                $('#Outputdb_select').append('<option value="'+data[i]+'">'+data[i]+'</option>');
            }   
        },
        complete : function(request, textStatus, errorThrown) {
        }
    });
}

function viewAttributeset()
{
    dbname = $('#Outputdb_select').val();
    datasetname = $('#Outputschema_select').val();
    $('#attributeList').empty();
    $('#output_attribute_modal #attribute_select').empty();
    
    filter_data.datasetName = datasetname;
    $.ajax({
        url: 'http://localhost:8080/HetroDS/webapi/HDBSchema/viewDataset/'+dbname+'/'+datasetname,
        type: 'POST',
        async:false,
        contentType : 'application/json',
        dataType : 'json',
        success : function(data){
            //alert(data.length);
            for(var i=0 ; i < data.length ; i++){
                $('#attributeList').append(
                   
                    '<input type="checkbox" name="attributes" value="'+data[i]['attribute_name']+'" style=" margin-top:10px;  transform: scale(1.5);">'+'&nbsp;&nbsp;&nbsp;'+data[i]['attribute_name']+'</input><br/>');

                $('#output_attribute_modal #attribute_select').append('<option value="'+data[i]['attribute_name']+'">'+data[i]['attribute_name']+'</option>');
                
            }
        },
        complete : function(request, textStatus, errorThrown) {
            //alert("Complete");
        }
        
    });
}
/// added by shachi to check attribute is object and if yes display extra textbox
function filterSelectObject(){
	attributeName = $('#attribute_select').val();
	dbname = $('#Outputdb_select').val();
    datasetname = $('#Outputschema_select').val();
    $('#output_attribute_modal #datatype').empty();
   
	$.ajax({
        url: 'http://localhost:8080/HetroDS/webapi/HDBSchema/viewDataset/'+dbname+'/'+datasetname,
        type: 'POST',
        async:false,
        contentType : 'application/json',
        dataType : 'json',
        success : function(data){
            for(var i=0 ; i < data.length ; i++){
            		if(attributeName==data[i]['attribute_name'])
            		{
            			if(data[i]['attribute_type']=="Object")
            			{
            				$('#output_attribute_modal #compositeValue').append('. &nbsp;<input type="text" id="hierarchyValue" size="12"/>');
            			}
            			else
            			{
            				$('#output_attribute_modal #compositeValue').empty();
                			$('#output_attribute_modal #datatype').append('('+data[i]['attribute_type']+' &nbsp;only)');
            			}
            			break;	
            		}
            }
        },
        complete : function(request, textStatus, errorThrown) {
            //alert("Complete");
        }
        
    });
}
////ends here	

function getFilterData()
{   
    filter_Name = $('#filterName').val(); 
    filter_data.filterName = filter_Name;   
    $.each($("input[name='attributes']:checked"), function(){            

        filter_data.projectContent.push($(this).val());

    });
    $('#attributeValue').val("");
    $('#addFilterAttributes').empty();
    filter_data.whereContent.length=0;
    $('#filterName').val('');
}

function getWhereData()
{
    $('#addFilterAttributes').empty();
    attributeName = $('#attribute_select').val();
    op = $('#operatorType').val();
    val = $('#attributeValue').val();
    connector = $('#connectorType').val();
    
    
    if($('#hierarchyValue').val())
    	attributeName = attributeName + '.' + $('#hierarchyValue').val();
    
    
    if(connector.selectedIndex == 0)
        connector = "";

    filter_data.whereContent.push({ 
        "Attribute": attributeName,
        "Operator": op,
        "Value": val,
        "Connector": connector
    });
    
    // $('#attributeValue').empty();
    $('#addFilterAttributes').empty();
    //$('#hierarchyValue').empty();
    $('.modal').find("input,textarea").val('');
    $('#output_attribute_modal #hierarchyValue').val('');
    
    document.getElementById("viewFilter_div").style.display = "";
    
    for(var i=0 ; i<filter_data.whereContent.length ; i++){
        
        if(filter_data['whereContent'][i]['Operator'] == '$eq') 
            op = '=';
        else if(filter_data['whereContent'][i]['Operator'] == '$gt') 
            op = '>';
        else if(filter_data['whereContent'][i]['Operator'] == '$lt') 
            op = '<';
        else if(filter_data['whereContent'][i]['Operator'] == '$gte') 
            op = '>=';
        else if(filter_data['whereContent'][i]['Operator'] == '$lte') 
            op = '<=';
        else if(filter_data['whereContent'][i]['Operator'] == '$ne') 
            op = '!=';
      $('#addFilterAttributes').append(
          '<tr>'+     
            '<td>'+filter_data.whereContent[i]['Attribute']+'</td>'+
            '<td>'+op+'</td>'+
            '<td>'+filter_data.whereContent[i]['Value']+'</td>'+
            '<td>'+filter_data.whereContent[i]['Connector']+'</td>'+
            '</tr>'
         );
    }
   // alert(JSON.stringify(filter_data));
}

function sendfilterData()
{
//    alert('sending...'+JSON.stringify(filter_data));
    $.ajax({
        url: 'http://localhost:8080/HetroDS/webapi/HDBOutput/storeFilter/'+dbname,
        type: 'POST',
        async:false,
        contentType : 'application/json',
        dataType : 'json',
        data : JSON.stringify(filter_data),
        success : function(data){
//            alert("Success of storeFilter");
//            getAllfilters();

        },
        complete : function(request, textStatus, errorThrown) {
            //alert("Complete of storeFilter");
        	getAllfilters();
        }
        
    });
    filter_data.filterName = "";   
    filter_data.projectContent.length = 0;
    filter_data.whereContent.length = 0;
}

function getAllfilters()
{
    dbname = $('#Outputdb_select').val();
    $('#filter_select').empty();
    $.ajax({
        url: 'http://localhost:8080/HetroDS/webapi/HDBOutput/getAllFilters/'+dbname,
        type: 'POST',
        async:false,
        contentType : 'application/json',
        dataType : 'json',
        success : function(data){
        	 $('#filter_select').append('<option value="Select Filter">Select Filter</option>');
            for(i=0;i<data.length;i++)
                $('#filter_select').append('<option value="'+data[i]['filterName']+'">'+data[i]['filterName']+'</option>');
        },
        complete : function(request, textStatus, errorThrown) {
//            alert("Complete of storeFilter");
        }
        
    });
}

function showFilterDetails()
{
	$('#view_filter_modal_body_id').empty();
    dbname = $('#Outputdb_select').val();
    filter_Name = $('#filter_select').val();
    if(filter_Name != 'Select Filter')
    {
        //alert("Inside showFilterDetails");
        $.ajax({
            url: 'http://localhost:8080/HetroDS/webapi/HDBOutput/getFilterDetails/'+dbname+'/'+filter_Name,
            type: 'POST',
            async:false,
            contentType : 'application/json',
            dataType : 'json',
            success : function(data){
              //alert('success');
                var filterData = '';
                var op = '';
                
                filterData = filterData +'<div class="panel with-nav-tabs panel-default" style=" padding-left: 20px; padding-right: 20px;padding-top: 10px; padding-bottom: 20px; border-width: 2px;"><h5><b>Filter Information</b></h5><hr>Filter Name: '+filter_Name+'<br/>Database: '+dbname+'<br/>Dataset: '+data['datasetName']+
                        '<br/>Condition : ';
                for(var i = 0 ;i < data['whereContent'].length ; i++ )
                {
                    if(data['whereContent'][i]['Operator'] == '$eq') 
                        op = '=';
                    else if(data['whereContent'][i]['Operator'] == '$gt') 
                        op = '>';
                    else if(data['whereContent'][i]['Operator'] == '$lt') 
                        op = '<';
                    else if(data['whereContent'][i]['Operator'] == '$gte') 
                        op = '>=';
                    else if(data['whereContent'][i]['Operator'] == '$lte') 
                        op = '<=';
                    else if(data['whereContent'][i]['Operator'] == '$ne') 
                        op = '!=';
                    
                    filterData = filterData +   data['whereContent'][i]['Attribute']+
                    ' '+op+' '+data['whereContent'][i]['Value'];
                    if(i+1 < data['whereContent'].length ) filterData = filterData +' '+ data['whereContent'][i]['Connector']+' ';
                    
                }
                
                filterData = filterData +'<br/>Projection : '+data['projectContent']+'</div>';
                
                $('#view_filter_modal_body_id').append(''+filterData+'</hr>');
            },
            complete : function(request, textStatus, errorThrown) {
    //            alert("Complete of storeFilter");
            }
            
        });
    }

}

function showRetrivedData()
{
//  alert("Inside showRetrivedData");
    $('#display_output_data_model #outputbox').val('');
    dbname = $('#Outputdb_select').val();
    filter_Name = $('#filter_select').val();
    datatype = $('#output_select').val();
    
    $.ajax({
        url: 'http://localhost:8080/HetroDS/webapi/HDBOutput/getData/'+dbname+'/'+filter_Name+'/'+datatype,
        type: 'POST',
        async:false,
        contentType : 'application/json',
        dataType : 'text',
        success : function(data){
        //  alert(JSON.stringify(data));
            $('#display_output_data_model #outputbox').val(data);

        },
        complete : function(request, textStatus, errorThrown) {
//            alert("Complete of storeFilter");
        }
        
    });
}

function saveTextAsFile()
{
    var textToSave = document.getElementById("outputbox").value;

    var textToSaveAsBlob = new Blob([textToSave], {type:"text/plain"});
   // alert(textToSaveAsBlob);
    var textToSaveAsURL = window.URL.createObjectURL(textToSaveAsBlob);
    //alert(textToSaveAsURL);
    var fileNameToSaveAs = document.getElementById("inputFileNameToSaveAs").value;
 
    var downloadLink = document.createElement("a");
    downloadLink.download = fileNameToSaveAs;
    downloadLink.innerHTML = "Download File";
    downloadLink.href = textToSaveAsURL;
    downloadLink.onclick = destroyClickedElement;
    downloadLink.style.display = "none";
    document.body.appendChild(downloadLink);
    downloadLink.click();
    $("#inputFileNameToSaveAs").val('');
}
 
function destroyClickedElement(event)
{
    document.body.removeChild(event.target);
}


function filterExist()
{
    filtername = $('#filterName').val();
    if($("#filter_select option[value='"+filtername+"']").length > 0)
    {
        alert("Filter already exists with same name!!");
        jQuery.noConflict();
       // $('#create_dataset_modal').modal('toggle');
        // $('#insert_attribute_modal').modal('toggle');
        // $('#create_datasetFromFile_modal').modal('toggle');
//        $('.modal').find("input,textarea").val('');
//        attribute_data.attributes.length = 0 ;
//        inside_data.length = 0;
//        document.getElementById("viewAtributes_div").style.display = "none";
        return;
        
    }
    else
    {
         jQuery.noConflict();
         $('#create_filter_modal').modal('toggle');
         viewAttributeset();
         $('#create_projection_modal').modal('toggle');
    }

}
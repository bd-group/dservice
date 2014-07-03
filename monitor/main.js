
var links = [[],
             [],
             [],
             []];
	var id;

function init1(){
	//alert(window.location.search);
//	document.getElementById("context").innerHTML="<iframe class='iframe' name='iframeContext'/>";
//	
//	var str = "<br/><br/>";
//	for(i=0;i<links.length;i++)
//		{
//		str+="<a target='iframeContext' class='links' href='"+links[i][1]+"'>"+links[i][0]+"</a><br/>";
//		}
//	document.getElementById("left").innerHTML=str;

	if(window.location.search==""){
			var random = Math.random();
			
			var today = new Date();
			var year=today.getFullYear(); 
    		var month=today.getMonth()+1;  
    		var date=today.getDate();    
//			var h=today.getHours(); 
//			var m=today.getMinutes(); 
//			var s=today.getSeconds(); 
			id = random;
			switch(month){
				case 1:month="01";break;
				case 2:month="02";break;
				case 3:month="03";break;
				case 4:month="04";break;
				case 5:month="05";break;
				case 6:month="06";break;
				case 7:month="07";break;
				case 8:month="08";break;
				case 9:month="09";break;
				default:break;
			}
			switch(date){
				case 1:date="01";break;
				case 2:date="02";break;
				case 3:date="03";break;
				case 4:date="04";break;
				case 5:date="05";break;
				case 6:date="06";break;
				case 7:date="07";break;
				case 8:date="08";break;
				case 9:date="09";break;
				default:break;
			}
			window.location.search="?city=XJ&date="+year+"-"+month+"-"+date +"&id=" +id;
		}else{
			id = window.location.search;
			var idsarr = id.split("&");
			var ids = idsarr[2];
			var idarr = ids.split("=");
			var result = idarr[1];
			id=result;
			

		}
	var links = ["query.png","Queue.png","report.png","space.png","frps.png","sfl.png","job.png","node.png","fops.png","fail.png","diskfrees.png","ds.png","loads.png","free.png", "mms_rate.png", "mms_bw.png", "mms_lat.png", "mms_misc.png", "mms_err.png"];
	for(var i = 0;i<19;i++)
	{
		var url = document.getElementById("url"+i).href;
		document.getElementById("url"+i).href =url + window.location.search;
		document.getElementById("img"+i).innerHTML='<img title="'+links[i]+'" width = "800px" height = "500px"  src="'+id+'/'+links[i]+'"/>';
	}
	
	var search = GetRequest();
	document.getElementById("citysd").value = search["city"];
	document.getElementById("dateid").value = search["date"];
}

function GetRequest() {
	  
	  var url = window.location.search; //获取url中"?"符后的字串
	   var theRequest = new Object();
	   if (url.indexOf("?") != -1) {
	      var str = url.substr(1);
	      strs = str.split("&");
	      for(var i = 0; i < strs.length; i ++) {
	         theRequest[strs[i].split("=")[0]]=(strs[i].split("=")[1]);
	      }
	   }
	   return theRequest;
	}

function sss()
{
	document.getElementById("hiddenid").value = id;
}
var myStyle=true;
function switchStyle()
{
	
	myStyle= !myStyle;
	

	document.getElementById("top").className = myStyle?"top1":"top2";
	
	
	document.getElementById("left").className = myStyle?"left1":"left2";
	
	
	document.getElementById("context").className = myStyle?"context1":"context2";
	
	s = myStyle?init1():init2();
}

var currDiv=null ;
function mouseOver(divid)
{
	if(currDiv!=null){
		currDiv.style.display="none";
	}
	currDiv=document.getElementById(divid);
	currDiv.style.display="block";
	
}


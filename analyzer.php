<?php
$conn = pg_connect('host=localhost user=postgres password=123');
$query = "select table_name from information_schema.tables WHERE table_name like 'act-%'";
$result = pg_exec($conn,$query);
$query = 'select max(time) as time from stack' ;  
$result1 = pg_exec($conn,$query);  
$resultfinal = null; 
if(pg_fetch_result($result1, 0,0)==null){
	 $time = new DateTime('1900-01-01');
	 $time = $time->format('Y-m-d H:i:s.u');
}
else{
	$data = pg_fetch_object($result1);	
	$time = $data->time;	
}
for($row=0; $row< pg_numrows($result); $row++){
  $action = pg_fetch_result($result, $row,0);
  $action = substr($action,4);
  $query = 'select * from "' . pg_fetch_result($result, $row,0).'" where time > '."'".$time."'".' order by time' ;  
  $result1 = pg_exec($conn,$query);  
  for($row1=0; $row1< pg_numrows($result1); $row1++){	 	 
	$data = pg_fetch_object($result1);	      
	$query = "insert into stack (action,time) values('".$action."','".$data->time."')";  
	pg_query($conn,$query);
  }
}
  $query = 'select * from stack where groupid is null order by time' ;  
  $result1 = pg_exec($conn,$query);  
  for($row1=0; $row1< pg_numrows($result1); $row1++){	
    $data = pg_fetch_object($result1);	  
	$datanow = new DateTime($data->time);
    $datamin = new DateTime($data->time);
    $datamin->sub(DateInterval::createFromDateString("10 minutes")); 
    $newgroupid = getgroupid($conn,$datamin,$datanow,$data);	
    $status =  getstatus($conn,$data,$newgroupid);
	$query = "update stack set groupid=$newgroupid, status=$status where time = '$data->time'";    	
	pg_query($conn,$query);	
	if($status==3){
		$query = "select * from stack where time = '$data->time'";    	
	    $resultfinal = pg_exec($conn,$query);  
		$datafinal = pg_fetch_object($resultfinal);	  
		$query = "update stack set sequence = '".$datafinal->sequence."' where groupid = '$datafinal->groupid'";    	
		pg_query($conn,$query);	 			
	}
  }
function getgroupid($conn,$datamin,$datanow,$data) {
	$out = 0;	
	$query = "select coalesce(max(groupid),0) from stack where time > '". $datamin->format('Y-m-d H:i:s.u') ."'
												           and time < '". $datanow->format('Y-m-d H:i:s.u') ."'" ;
	$result = pg_exec($conn,$query);	
	if(pg_fetch_result($result, 0,0)==0){	  
	  $query = 'select coalesce(max(groupid),0) from stack where groupid > 0';
	  $result = pg_exec($conn,$query);
	  $out = pg_fetch_result($result, 0,0) + 1; 
	}
	else{
		
		$query = "select * from flow WHERE actname = '".strtoupper($data->action)."'";
		$result2 = pg_exec($conn,$query);	
	    $data2 = pg_fetch_object($result2);
		if ($data2->sequence != 1){			
			for($row1=0; $row1< pg_numrows($result2); $row1++){
				$searchgi = pg_fetch_result($result2, $row1,"groupid");				
			    echo "<br/>".pg_fetch_result($result2, $row1,"actname");				
				
				$counter=$data2->sequence - 1;
			for($row=$counter; $row>0; $row--){
				$query = "select * from flow WHERE groupid = '".$searchgi."' and sequence = '".$row."'";
                $result3 = pg_exec($conn,$query);
				$data3 = pg_fetch_object($result3);
				$fields = getfieldsdef($conn,"ACT-".$data3->actname);				
				$query = "select * from \"act-". strtolower($data->action). "\" where time = '". $data->time."'";
				$result4 = pg_exec($conn,$query);	
                $data4 = pg_fetch_object($result4);				
				$fieldsquery = '';
				foreach ($fields as &$value) {
				   $prop = strtolower($value);
                    print_r($datanow);
				    if(property_exists($data4,$prop)){ 
                       if($data4->$prop===''){
                          $out = pg_fetch_result($result, 0,0) + 1; 	
				          return $out;
					   }  
				       $fieldsquery = $fieldsquery." and ".$prop." = '". $data4->$prop."'";		 
				    }
				
				}
				$query = "select * from \"act-". strtolower($data3->actname) ."\" where time > '". $datamin->format('Y-m-d H:i:s.u') ."'
								                                    and time < '". $data->time."'".$fieldsquery." 
																  
						  and time in ( select time from stack where status <> 0 and groupid 
						           in ( select groupid  from stack where status <> 0 
                                          and groupid not in (select groupid from stack where status = 3)))";	
                echo "query secondo flow: <br/>" . $query;
               	$result3 = pg_exec($conn,$query);	
                $data3 = pg_fetch_object($result3);
                                
				if(pg_numrows($result3)>0){
					echo "found";
				   $query = "select * from stack where time = '". $data3->time."'";	  
				   $result3 = pg_exec($conn,$query);	
                                   $data3 = pg_fetch_object($result3);			
                                   $out = $data3->groupid;	
                                   return $out;
				}
				else{
				echo "not found";
				   $out = pg_fetch_result($result, 0,0) + 1; 	
				   //return $out;
				}
			}
			    
			}
						
		}
		else{
			$query = "select * from \"act-".$data->action."\" where time = '$data->time'";
			echo "query error <br/>" . $query;
			$result1 = pg_exec($conn,$query);  
			$data1 = pg_fetch_object($result1);	  
			$fields = getfieldsdef($conn,"ACT-".$data->action);
			$fieldsquery = '';
			foreach ($fields as &$value) {	
			$prop =	strtolower($value);
			if(property_exists($data1,$prop)){ 
               if($data1->$prop===''){
                  $out = pg_fetch_result($result, 0,0) + 1; 	
			      return $out;
                   
               }
               $fieldsquery = $fieldsquery." and ".$prop." = '". $data1->$prop."'";		 
			    
			}	
			
			}
			$query = "select max(time) from \"act-".$data->action."\" where time > '". $datamin->format('Y-m-d H:i:s.u') ."'
														  and time < '". $data->time."'".$fieldsquery." ";
			$result3 = pg_exec($conn,$query);
			$data1 = pg_fetch_result($result3, 0,0);	  
			if($data1==null){
				$out = pg_fetch_result($result, 0,0) + 1; 
			}
			else{
				$query = "
				select groupid from stack where time = '".$data1."' and status <> 0
				and groupid in ( 	
                select groupid  from stack where status <> 0 
                         and groupid not in (select groupid from stack where status = 3))";
				$resultcopy = pg_exec($conn,$query);
				if(pg_numrows($resultcopy)!=0){
					$data1 = pg_fetch_result($resultcopy, 0,0);				
					if($data1==null){
						$out = pg_fetch_result($result, 0,0) + 1; 
					}
					else{
					   $out = pg_fetch_result($resultcopy,0,0);				
					}
				} 
				else{
					$out = pg_fetch_result($result, 0,0) + 1; 
				}				
			}			
		}		
		
    
	}
	return $out;
}
function getstatus($conn,$data,$newgroupid) {
	$out = 0;        
	$query = "select * from flow WHERE actname = '".strtoupper($data->action)."'";
	$result2 = pg_exec($conn,$query);	
	$data2 = pg_fetch_object($result2);		
	$datamin = new DateTime($data->time);
	$datamin->sub(DateInterval::createFromDateString("10 minutes"));
	$query = "select actname from flow where sequence = (select max(sequence) from flow)";
	$resultmax = pg_exec($conn,$query);
	$actfinal = strtolower(pg_fetch_result($resultmax, 0,0));
	for($row=0; $row< pg_numrows($result2); $row++){
        if ($data2->sequence == 1){
			$fields = getfieldsdef($conn,"ACT-".$data->action);
			$fieldsquery = '';
			$query = "select * from \"act-".$data->action."\" where time = '$data->time'";
			$result1 = pg_exec($conn,$query);  
			$data1 = pg_fetch_object($result1);	  
			foreach ($fields as &$value) {	
			   $prop = strtolower($value);
               if($prop===''){                                  
                  return 0;
               }
			   $fieldsquery = $fieldsquery." and ".$prop." = '". $data1->$prop."'";		
			}
			$query = "select max(time) from \"act-".$data->action."\" where time > '". $datamin->format('Y-m-d H:i:s.u') ."'
														  and time < '". $data->time."'".$fieldsquery." ";
			$result3 = pg_exec($conn,$query);
			$data1 = pg_fetch_result($result3, 0,0);	  
			if($data1==null){
			    $query = "select * from response where time = '".$data->time."' and esito = true";
			    $result3 = pg_exec($conn,$query);
			    if(pg_numrows($result3)>0){
			       $out = 1;     
			    }
				else{
				   $out = 4;     
				}
			}
			else{
				$query = "
				select groupid from stack where time = '".$data1."' and status <> 0
				and groupid in ( 	
                select groupid  from stack where status <> 0 
                         and groupid not in (select groupid from stack where status = 3))";
				$resultcopy = pg_exec($conn,$query);
				if(pg_numrows($resultcopy)!=0){
					$data1 = pg_fetch_result($resultcopy, 0,0);				
					if($data1==null){
					    $query = "select * from response where time = '".$data->time."' and esito = true";
			            $result3 = pg_exec($conn,$query);
			            if(pg_numrows($result3)>0){
			               $out = 1;     
			            }
			            else{
			               $out = 4;      
			            }
					}
					else{
					   $query = "select * from response where time = '".$data->time."' and esito = true";
			           $result3 = pg_exec($conn,$query); 
			           if(pg_numrows($result3)>0){
					      $out = 2;
			           }
			           else{
			               $out = 4;      
			            }
					}
				} 
				else{
				    $query = "select * from response where time = '".$data->time."' and esito = true";
			        $result3 = pg_exec($conn,$query);
			        if(pg_numrows($result3)>0){
			           $out = 1;     
			        }
			        else{
			           $out = 4;      
			        }
				}
				
			}					
		}
		else{
			$fields = getfieldsdef($conn,"ACT-".$data->action);
			$fieldsquery = '';
			//verifico se copia in act-*
			$query = "select * from \"act-".$data->action."\" where time = '$data->time'";
			$result1 = pg_exec($conn,$query);  
			$data4 = pg_fetch_object($result1);	
			print_r($data4);
			$fields = getfieldsdef($conn,"ACT-".$data->action);
			$fieldsquery = '';
			foreach ($fields as &$value) {	
			    $prop =	strtolower($value);
			    if($prop===''){
                   return 0;
                }    
				$fieldsquery = $fieldsquery." and ".$prop." = '". $data4->$prop."'";		 
			}
            $query = "select * from \"act-".$data->action."\" where time > '". $datamin->format('Y-m-d H:i:s.u') ."'
												              and time < '". $data->time."'".$fieldsquery." ";			
	        $result3 = pg_exec($conn,$query);				
			$datacopy = pg_fetch_object($result3);
			$resultcopy = null;
			$copytime = null;
			if(pg_numrows($result3)>0){
				for($row1=0; $row1< pg_numrows($result3); $row1++){	
                    $copytime = pg_fetch_result($result3, $row1,'time');
                    $query = "select * from stack where time = '$copytime' and status <> 0 and groupid in ( 	
                              select groupid  from stack where status <> 0 
                              and groupid not in (select groupid from stack where status = 3))";								
					$resultcopy = pg_exec($conn,$query);		
	            }				
			}
			if(pg_numrows($result3)>0 && pg_numrows($resultcopy) > 0){
				$query = "select * from stack where status <> 0 
												and action = '$actfinal'
												and groupid = '$newgroupid'";
				$resultfinal = pg_exec($conn,$query);
				if(pg_numrows($resultfinal)>0){
				   $out=0;			
				}else{
				   $query = "select * from response where time = '".$data->time."' and esito = true";
			           $result3 = pg_exec($conn,$query); 
			           if(pg_numrows($result3)>0){
					      $out = 2;
			           }
			           else{
			               $out = 4;      
			            }			
				}				
			}
			else{			
			    $searchgi = pg_fetch_result($result2, $row,"groupid");				
				$searchseq = pg_fetch_result($result2, $row,"sequence");				
				$counter=$data2->sequence - 1;
				for($row2=$counter; $row2>0; $row2--){
					$query = "select * from flow WHERE groupid = '".$searchgi."' and sequence = '".$row2."'";					
					$result3 = pg_exec($conn,$query);
					$data3 = pg_fetch_object($result3);
					$fields = getfieldsdef($conn,"ACT-".$data3->actname);
					$fieldsquery = '';
					$notwrong = 0;
					foreach ($fields as &$value) {	
					$prop =	strtolower($value);
					if(property_exists($data4,$prop)){  
					   $fieldsquery = $fieldsquery." and ".$prop." = '". $data4->$prop."'";		 
					}
					}
					$query = "select * from \"act-". strtolower($data3->actname) ."\" where time > '". $datamin->format('Y-m-d H:i:s.u') ."'
													    and time < '". $data->time."'".$fieldsquery;
					
					$result3 = pg_exec($conn,$query);					
					if(pg_numrows($result3)>0){
					
					   for($row1=0; $row1<pg_numrows($result3); $row1++){
						   $time = pg_fetch_result($result3, $row1,'time');
						   $query = "select * from stack where time = '". $time."'";
						   $result4 = pg_exec($conn,$query);	
						   $data4 = pg_fetch_object($result4);
						   if($data4->status !=0 && $data4->status !=4 ){
							   echo "<br/>first".$data4->groupid;
							   echo "<br/>second".$newgroupid;
							   if($data4->groupid == $newgroupid){
							      $notwrong = $notwrong + 1;								  
						       }					       
						   }
					   }
					   if($notwrong >= 1 ){
						   $query = "select actname from flow WHERE sequence > ".$data2->sequence. " and groupid = " . $searchgi ;
						   echo "<br/> query final".$query;
						   $resultfinal = pg_exec($conn,$query);							   	  
						   if (pg_numrows($resultfinal)==0){
							  $query = "select * from response where time = '".$data->time."' and esito = true";
			                  $result3 = pg_exec($conn,$query);
			                  if(pg_numrows($result3)>0){
								 $query = "update stack set sequence = ".$searchgi." where time = '$data->time'";    	
	                             pg_query($conn,$query);	 
			                     return 3;     
			                  }
			                  else{
			                     return 4;      
			                  }
						   }
						   else{
						       $query = "select * from response where time = '".$data->time."' and esito = true";						       
			                   $result3 = pg_exec($conn,$query);
			                  if(pg_numrows($result3)>0){
			                     return 1;     
			                  }
			                  else{
			                     return 4;      
			                  }
						   }
					   }
					   else {
						   echo "<br/> primo 0"; 
					       $out=0;	//return 0;
					   }
					}
					else{
					   echo "<br/> secondo 0";
					   $out=0;	
					   //return $out;
					}
				}
			}
		}
	}
	return $out;
}
function getfieldsdef($conn,$action) {
	$out = []; 
	$query = "select * from actdef WHERE action = '".strtoupper($action)."'";
    $result = pg_exec($conn,$query);	
	for($row=0; $row< pg_numrows($result); $row++){			 
		array_push($out,pg_fetch_result($result, $row,1));
	}
	return $out;
}
pg_close();
echo "analisi completata";

?>

<html>
<head>
<title>Find bamboo nodes</title>
</head>
<body>
Enter information in the dialog boxes below to find nodes that match the criteria you specify.<br />
Leave a field blank if you want it ignored.
<form action="index.php" method="get">
    Build:  <input type="text" name="build" />
    <select name="build_op">
     <option value="=">=
     <option value=">">>
     <option value="<"><
     <option value=">=">>=
     <option value="<="><=
    </select><br />
    Port: <input type="text" name="port" />
    <select name="port_op">
     <option value="=">=
     <option value=">">>
     <option value="<"><
     <option value=">=">>=
     <option value="<="><=
    </select><br />
    Uptime (in seconds): <input type="text" name="uptime" />
    <select name="uptime_op">
     <option value="=">=
     <option value=">">>
     <option value="<"><
     <option value=">=">>=
     <option value="<="><=
    </select><br />
    Storage (in Mbs): <input type="text" name="storage" />
    <select name="storage_op">
     <option value="=">=
     <option value=">">>
     <option value="<"><
     <option value=">=">>=
     <option value="<="><=
    </select><br />
    X Coordinate: <input type="text" name="x_coord" />
    <select name="x_coord_op">
     <option value="=">=
     <option value=">">>
     <option value="<"><
     <option value=">=">>=
     <option value="<="><=
    </select><br />
    Y Coordinate: <input type="text" name="y_coord" />
    <select name="y_coord_op">
     <option value="=">=
     <option value=">">>
     <option value="<"><
     <option value=">=">>=
     <option value="<="><=
    </select><br />
    Estimate of network size: <input type="text" name="estimate" />
    <select name="estimate_op">
     <option value="=">=
     <option value=">">>
     <option value="<"><
     <option value=">=">>=
     <option value="<="><=
    </select><br />
    <input type="submit" name="submit" value="Search" />
    <br />
</form>

<?php

$filename = 'nodes.info';
$fp = fopen($filename, "r");
$contents = fread($fp, filesize($filename));
fclose($fp);

$node_info[0] = false;
$current_position = 1;

$next_pos = strpos($contents, "\n");
while($next_pos !== false)
{
  //Get a line from the file
  $line = trim(substr($contents, 0, $next_pos));
  //Remove the line we just got
  $contents = ltrim(substr($contents, $next_pos));

  //Break line up into a manageable array
  $line_array = tokenize_line($line);
  //Put this line's contents into the nodeinfo array
  for($i = 0; $i < sizeof($line_array); $i++) { 
    $node_info["$current_position:$i"] = $line_array[$i];
  }

  //Get next position of \n, indicating we have another line to read
  $next_pos = strpos($contents, "\n");
  $current_position++;
}

//Check each user input to make sure the condition is satisfied
$node_info = check_position($node_info, $current_position, 1, trim($_GET['build']), $_GET['build_op']);
$node_info = check_position($node_info, $current_position, 4, trim($_GET['port']), $_GET['port_op']);
$node_info = check_position($node_info, $current_position, 5, trim($_GET['uptime']), $_GET['uptime_op']);
$node_info = check_position($node_info, $current_position, 6, trim($_GET['storage']), $_GET['storage_op']);
$node_info = check_position($node_info, $current_position, 7, trim($_GET['x_coord']), $_GET['x_coord_op']);
$node_info = check_position($node_info, $current_position, 8, trim($_GET['y_coord']), $_GET['y_coord_op']);
$node_info = check_position($node_info, $current_position, 9, trim($_GET['estimate']), $_GET['estimate_op']);

$num_matching = 0;
echo "<p />Matching nodes:<br />";
for($i = 1; $i < $current_position; $i++) {
  if($node_info["$i:0"] == true)
    {
      $one = "$i:1";
      $two = "$i:2";
      $three = "$i:3";
      $four = "$i:4";
      $five = "$i:5";
      $six = "$i:6";
      $seven = "$i:7";
      $eight = "$i:8";
      $nine = "$i:9";
      $num_matching++;
      echo "$num_matching - $node_info[$two] $node_info[$three]:$node_info[$four]<br />";
    }
}
echo "<p />$num_matching nodes matched the search criteria.<br />";

function tokenize_line($line)
{
  $return_array[0] = true;
  $current_pos = 1;

  $next_space = strpos($line, " ");
  while($next_space !== false)
    {
      //Get a token from the file
      $token = trim(substr($line, 0, $next_space));

      //Remove the token we just got
      $line = ltrim(substr($line, $next_space));
      $return_array[$current_pos] = $token;

      //Get next position of token
      $next_space = strpos($line, " ");
      $current_pos++;
    }

  return $return_array;
}


//Checks the position in $node_info to see if it correctly satisfies the value and operator
//It sets the boolean of the array to false if the test fails, indicating this node isn't matching.
function check_position($node_info, $info_length, $position, $value, $operator)
{
  if(!(isset($value)) || empty($value) || $value == "" || is_null($value))
    {
      return $node_info;
    }

  for($i = 1; $i < $info_length; $i++)
    {
      if($node_info["$i:0"] != false)
        {
          if($operator == ">")
            {
              if(!($node_info["$i:$position"] > $value))
                {
                  $node_info["$i:0"] = false;
                }
            }
          elseif ($operator == ">=")
            {
              if(!($node_info["$i:$position"] >= $value))
                {
                  $node_info["$i:0"] = false;
                }
            }
          elseif ($operator == "<")
            {
              if(!($node_info["$i:$position"] < $value))
                {
                  $node_info["$i:0"] = false;
                }
            }
          elseif ($operator == "<=")
            {
              if(!($node_info["$i:$position"] <= $value))
                {
                  $node_info["$i:0"] = false;
                }
            }
          else
            {
              if(!($node_info["$i:$position"] == $value))
                {
                  $node_info["$i:0"] = false;
                }
            }
        }
    }

  return $node_info;
}

?>

</body>
</html>


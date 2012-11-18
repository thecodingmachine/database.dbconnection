<?php
/**
 * The proxy server for database.
 * Executes a passed database method and returns the result.
 * The user must be logged in Mouf to be able to run this script. 
 */

if (!isset($_REQUEST["selfedit"]) || $_REQUEST["selfedit"]!="true") {
	require_once '../../../../../Mouf.php';
} else {
	require_once '../../../../../mouf/MoufManager.php';
	MoufManager::initMoufManager();
	require_once '../../../../../MoufUniversalParameters.php';
	require_once '../../../../../mouf/MoufAdmin.php';
}

// Note: checking rights is done after loading the required files because we need to open the session
// and only after can we check if it was not loaded before loading it ourselves...
require_once '../../../../../mouf/direct/utils/check_rights.php';

$encode = "php";
if (isset($_REQUEST["encode"]) && $_REQUEST["encode"]="json") {
	$encode = "json";
}

$instance = $_REQUEST["instance"];
$method = $_REQUEST["method"];
$args = $_REQUEST["args"];
if (get_magic_quotes_gpc()==1)
{
	$instance = stripslashes($instance);
	$method = stripslashes($method);
	$args = stripslashes($args);
}

$dbConnection = MoufManager::getMoufManager()->getInstance($instance);
/* @var $dbConnection DB_ConnectionInterface */

$result = call_user_func_array(array($dbConnection, $method), unserialize($args));

if ($encode == "php") {
	echo serialize($result);
} elseif ($encode == "json") {
	echo json_encode($result);
} else {
	echo "invalid encode parameter";
}

?>
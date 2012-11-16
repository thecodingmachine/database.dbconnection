<?php 
require_once dirname(__FILE__).'/../../../../../Mouf.php';

/*$mysqlConn = Mouf::getMysqlConnection();
$mysqlConn->connect();
var_dump($mysqlConn->getTableInfo("test"));*/

$cacheConn = Mouf::getDbCachedConnection();
$cacheConn->connect();
var_dump($cacheConn->getTableFromDbModel("test"));
?>
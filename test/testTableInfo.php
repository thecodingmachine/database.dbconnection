<?php 
require_once("../../../dbconnectionsettings/1.0/ConnectionSettings.php");
require_once("../MySqlConnection.php");
require_once("../PgSqlConnection.php");

$conn = new MySqlConnection();
$conn->host = "localhost";
$conn->dbname = "test";
$conn->user = "root";
$conn->connect();

/*$connPg = new PgSqlConnection();
$connPg->host = "localhost";
$connPg->dbname = "demo";
$connPg->user = "demo";
$connPg->password = "demo";
$connPg->connect();
*/

//var_dump($conn->getListOfTables());
//var_dump($conn->isTableExist("test"));
//var_dump($conn->isTableExist("test2"));
var_dump($conn->getTableFromDbModel("test"));
?>
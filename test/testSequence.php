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

//var_dump($conn->getAll("SELECT * FROM users"));

//var_dump($conn->getOne("SELECT count(1) FROM users"));

//var_dump($conn->getTableInfo("users"));

/*$id = new Column();
$id->name = "id";
$id->autoIncrement = true;
$id->isPrimaryKey = true;
$id->type = "int";
$id->nullable = false;

$name = new Column();
$name->name = "name";
$name->autoIncrement = false;
$name->type = "varchar(255)";
$name->nullable = false;

$conn->createTable("test", array($id, $name), false);
*/
try {
$conn->nextId("test");
} catch (PDOException $e) {
	var_dump($e);
}
?>
<?php
namespace Mouf\Database\DBConnection; 

use PHPUnit_Framework_TestCase;

require_once('../../../../../../autoload.php');
require_once('../../../config.php');

class MsSqlConnectionTest extends PHPUnit_Framework_TestCase {
	
	private $dbConnection;
	
	function __construct() {
       parent::__construct();
       
       $this->dbConnection = new MsSqlConnection();
       $this->dbConnection->dbname = "test";
       $this->dbConnection->user = DBUSER;
       $this->dbConnection->password = DBPASSWORD;
       $this->dbConnection->host = DBHOST;
       $this->dbConnection->instance = DBINSTANCENAME;
       $this->dbConnection->odbcDriver = ODBCDRIVER;
    }

    /**
	 * Returns the constraints on table "table_name" and column "column_name" if "column_name"is given
	 * this function returns an array of arrays of the form:
	 * ("table2"=>"name of the constraining table", "col2"=>"name of the constraining column", "col1"=>"name
	 * of the constrained column")
	 *
	 * @param string $table_name
	 * @param string $column_name
	 * @return unknown
	 */
	public function testGetConstraintsOnTable() {
		$constraints = $this->dbConnection->getConstraintsOnTable("countries");
		$this->assertEquals("users", $constraints[0]['table2']);
		$this->assertEquals("country_id", $constraints[0]['col2']);
		$this->assertEquals("id", $constraints[0]['col1']);
	}
	
	/**
	 * Returns the constraints on table "table_name" and column "column_name" if "column_name"is given
	 * this function returns an array of arrays of the form:
	 * ("table1"=>"name of the constrained table", "col1"=>"name of the constrained column", "col2"=>"name
	 * of the constraining column")
	 *
	 * @param string $table_name
	 * @param string $column_name
	 * @return unknown
	 */
	public function testGetConstraintsFromTable() {
		$constraints = $this->dbConnection->getConstraintsFromTable("users");
		$this->assertEquals("countries", $constraints[0]['table1']);
		$this->assertEquals("id", $constraints[0]['col1']);
		$this->assertEquals("country_id", $constraints[0]['col2']);
	}
	
	/**
	 * Returns the table columns.
	 *
	 */
	public function testGetTableInfo() {
		$users = $this->dbConnection->getTableInfo("users");
		$this->assertEquals(3, count($users));
	}
	
	/**
     * Checks if the database with the given name exists.
     * Returns true if it exists, false otherwise.
     * Of course, a connection must be established for this call to succeed.
     * Please note that you can create a connection without providing a dbname.
     * 
     * @param string $dbName
     * @return bool
     */
    public function testCheckDatabaseExists() {
    	$isDatabaseExist = $this->dbConnection->checkDatabaseExists("test");
    	$this->assertEquals(true, $isDatabaseExist);
    }
    
    /**
	 * Returns true if the table exists, false if it does not.
	 *
	 */
	public function testIsTableExist() {
		$isTableExit = $this->dbConnection->isTableExist("users");
		$this->assertEquals(true, $isTableExit);
		$isTableExit = $this->dbConnection->isTableExist("usersfdsfsd");
		$this->assertEquals(false, $isTableExit);
	}
	
	/**
	 * Returns a list of table names.
	 *
	 * 
	 * @param $ignoreSequences boolean: for some databases, sequences are managed with tables. If true, those tables will be ignored. Default is true.
	 * @return array<string>
	 */
	public function testGetListOfTables() {
		$tables = $this->dbConnection->getListOfTables();
		$this->assertEquals(true, array_search("users", $tables) !== false);
		$this->assertEquals(true, array_search("countries", $tables) !== false);
		
	}
	
	/**
	 * Returns a table object (Table) from the database. 
	 *
	 */
	public function testGetTableFromDbModel() {
		$table = $this->dbConnection->getTableFromDbModel("users");
		$this->assertEquals(3, count($table->columns));
		$this->assertEquals(true, $table->columns[0]->autoIncrement);
	}
    
	public function testGetAll() {
		$result = $this->dbConnection->getAll("SELECT * FROM users");
		//var_dump($result);
	}
}

?>
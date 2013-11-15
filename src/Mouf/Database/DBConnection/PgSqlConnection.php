<?php
namespace Mouf\Database\DBConnection; 

use PDO;
use PDOException;

/**
 * A class wrapping a connection to a PgSQL database through PDO, with additional goodies (introspection support)
 *
 * @Component
 * @Renderer { "smallLogo":"vendor/mouf/database.dbconnection/icons/database_small.png" }
 */
class PgSqlConnection extends AbstractDBConnection {
	
	/**
	 * The host for the database.
	 * This is the IP or the URL of the server hosting the database.
	 *
	 * @Property
	 * @Compulsory
	 * @var string
	 */
	public $host;
	
	/**
	 * The port for the database.
	 * Keep empty to use default port.
	 *
	 * @Property
	 * @var int
	 */
	public $port;
	
	/**
	 * Database user to use when connecting.
	 *
	 * @Property
	 * @var string
	 */
	public $user;
	
	/**
	 * Password to use when connecting.
	 *
	 * @Property
	 * @var string
	 */
	public $password;
	
	/**
	 * Charset used to communicate with the database.
	 * The database will translate any string into this charset before sending us the string.
	 * If not set, this will default to UTF-8
	 *
	 * @Property
	 * @var string
	 */
	//public $charset;
	
	/**
	 * Whether a persistent connection is used or not.
	 * If this application is used on the web, you should choose yes. The database connection
	 * will not be closed when the script stops and will be reused on the next connection.
	 * This will help improve your application's performance. 
	 *
	 * This defaults to "true"
	 * 
	 * @Property
	 * @var boolean
	 */
	public $isPersistentConnection;
	
	/**
	 * Returns the DSN for this connection.
	 *
	 * @return string
	 */
	public function getDsn() {
		if ($this->dbname) {
			$dsn = "pgsql:host=".$this->host.";dbname=".$this->dbname.";";
		} else {
			$dsn = "pgsql:host=".$this->host.";dbname=template0;";
		}
		if (!empty($this->port)) {
			$dsn .= "port=".$this->port.";";
		}
		/*$charset = $this->charset;
		if (empty($charset)) {
			$charset = "UTF-8";
		}
		$dsn .= "charset=".$charset.";";
		*/
		return $dsn;
	}

	/**
	 * Returns the username for this connection (if any).
	 *
	 * @return string
	 */
	public function getUserName() {
		return $this->user;
	}

	/**
	 * Returns the password for this connection (if any).
	 *
	 * @return string
	 */
	public function getPassword() {
		return $this->password;
	}
	
	/**
	 * Returns an array of options to apply to the connection.
	 *
	 * @return array
	 */
	public function getOptions() {
		$options = array();
		if ($this->isPersistentConnection != "No") {
			$options[PDO::ATTR_PERSISTENT] = true;
		}
		$options[PDO::ATTR_ERRMODE] = PDO::ERRMODE_EXCEPTION;
		return $options;
	}
	
	
	/**
	 * Empty constructor
	 *
	 */
	public function __construct() {
		parent::__construct();
	}
	
	/**
	 * Returns Root Sequence Table for $table_name
	 * i.e. : if "man" table inherits "human" table , returns "human" for Root Sequence Table
	 * !! Warning !! Child table must share Mother table's primary key
	 * @param string $table_name
	 * @return string
	 */
	public function findRootSequenceTable($table_name){
		
		$child_table = $table_name;
		$root_table = $table_name;
		while ($parent_table=$this->getParentTable($child_table)) {
			$root_table = $parent_table;
			$child_table = $parent_table;
		}
		if ($root_table != null)
		return $root_table;
	
	}
	
	/**
	 * Returns the parent table (if the table inherits from another table).
	 * For DB systems that do not support inheritence, returns the table name.
	 *
	 * @param string $table_name
	 * @return string
	 */
	public function getParentTable($table_name){
		$sql = "SELECT par.relname as parent_table FROM pg_class tab
				Left JOIN pg_inherits inh  ON inh.inhrelid = tab.oid 
				left JOIN pg_class par ON inh.inhparent = par.oid 
				WHERE tab.relname='$table_name'";

		$result = $this->getAll($sql);
		if (count($result)==1) {
			$result = $result[0]['parent_table'];
		}elseif (count($result)==0){
			$result = null;
		}else{
			throw new DBConnectionException('Several parents found for table '.$table_name.'<br />\n
					-> Error : this behavior is not managed by TDBM.');
		}
		return $result;
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
	public function getConstraintsOnTable($table_name,$column_name=false) {
		if ($column_name)
		{
			$sql = "SELECT t1.relname AS table2, c1.attname AS col2, c2.attname AS col1 FROM
				pg_attribute c2 JOIN pg_class t2 JOIN
				(pg_constraint con JOIN 
				(pg_class t1 JOIN pg_attribute c1 ON t1.oid = c1.attrelid)
				ON con.conrelid = t1.oid AND con.conkey[1]=c1.attnum)
				ON t2.oid = con.confrelid ON c2.attrelid = t2.oid AND con.confkey[1]=c2.attnum
			WHERE t2.relname='$table_name' AND c2.attname='$column_name'";
		}
		else
		{
			$sql = "SELECT t1.relname AS table2, c1.attname AS col2, c2.attname AS col1 FROM
				pg_attribute c2 JOIN pg_class t2 JOIN
				(pg_constraint con JOIN 
				(pg_class t1 JOIN pg_attribute c1 ON t1.oid = c1.attrelid)
				ON con.conrelid = t1.oid AND con.conkey[1]=c1.attnum)
				ON t2.oid = con.confrelid ON c2.attrelid = t2.oid AND con.confkey[1]=c2.attnum
			WHERE t2.relname='$table_name'";
		}

		$result = $this->getAll($sql);

		return $result;
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
	function getConstraintsFromTable($table_name,$column_name=false) {
		if ($column_name)
		{
			$sql = "SELECT t2.relname AS table1, c2.attname AS col1, c1.attname AS col2 FROM
				pg_attribute c2 JOIN pg_class t2 JOIN
				(pg_constraint con JOIN 
				(pg_class t1 JOIN pg_attribute c1 ON t1.oid = c1.attrelid)
				ON con.conrelid = t1.oid AND con.conkey[1]=c1.attnum)
				ON t2.oid = con.confrelid ON c2.attrelid = t2.oid AND con.confkey[1]=c2.attnum
			WHERE t1.relname='$table_name' AND c1.attname='$column_name'";
		}
		else
		{
			$sql = "SELECT t2.relname AS table1, c2.attname AS col1, c1.attname AS col2 FROM
				pg_attribute c2 JOIN pg_class t2 JOIN
				(pg_constraint con JOIN 
				(pg_class t1 JOIN pg_attribute c1 ON t1.oid = c1.attrelid)
				ON con.conrelid = t1.oid AND con.conkey[1]=c1.attnum)
				ON t2.oid = con.confrelid ON c2.attrelid = t2.oid AND con.confkey[1]=c2.attnum
			WHERE t1.relname='$table_name'";
		}

		$result = $this->getAll($sql);

		return $result;
	}

	
	/**
	 * Creates a new table in the database.
	 *
	 * @param Table $table The table to create
	 * @param boolean $dropIfExist whether the table should be dropped or not if it exists.
	 */
	public function createTable(Table $table, $dropIfExist = false) {
		throw new DBConnectionException("Method not implemented yet");
	}
	
	/**
	 * Creates a new index in the database.
	 *
	 * @param string $tableName
	 * @param array<string> $columnsList
	 * @param boolean $isUnique whether the index is unique or not.
	 * @param string $indexName The index name, generated if not specified.
	 */
	public function createIndex($tableName, $columnsList, $isUnique, $indexName=null) {
		throw new DBConnectionException("Method not implemented yet");
	}
	
	/**
	 * Returns the next Id from the sequence.
	 *
	 * @param string $seq_name The name of the sequence
	 * @param boolean $onDemand If true, if the sequence does not exist, it will be created.
	 * @return int The next value of the sequence
	 */
	public function nextId($seq_name, $onDemand = true) {
		$realSeqName = $this->getSequenceName($seq_name);
		try {
			$result = $this->getOne("SELECT NEXTVAL(".$this->quoteSmart($realSeqName).") as nextval");
		} catch (PDOException $e) {
			if ($e->getCode() == '42P01' && $onDemand) {
             	// ONDEMAND TABLE CREATION
             	$result = $this->createSequence($seq_name);

             	return 1;
        	} else {
        		throw $e;	
        	}
		}
		
		return $result;
	}
	
    /**
     * Creates a sequence with the name specified.
     * Note: The name is transformed be the getSequenceName method.
     * By default, if "mytable" is passed, the name of the sequence will be "mytable_pk_seq".
     *
     * @param string $seq_name
     */
    public function createSequence($seq_name) {
    	$realSeqName = $this->getSequenceName($seq_name);
    	$sql = 'CREATE SEQUENCE '.$realSeqName;
    	$this->exec($sql);
    }
	
    /**
	 * Returns a table object (Table) from the database. 
	 *
	 * @param string $tableName
	 * @return Table
	 */
	public function getTableFromDbModel($tableName) {
		// Check that the table exist.
		if  (!$this->isTableExist($tableName)) {
			throw new DBConnectionException("Unable to find table '".$tableName."'"); 
		}

		$dbTable = new Table($tableName);
		
		// Get the columns
		$tableInfo = $this->getTableInfo($tableName);

		// Map the columns to Column objects
		foreach ($tableInfo as $column) {
			$dbColumn = new Column();
			$dbColumn->name = $column['column_name'];
			// Let's compute the type:
			$type = $column['udt_name'];
			if ($type == "varchar") {
				$type .= '('.$column['character_maximum_length'].')';
			}
			$dbColumn->type = $type;
			$dbColumn->nullable = $column['is_nullable'] == 'YES'; 
			$dbColumn->default = $column['column_default'];
			
			// TODO: initialize the Autoincrement value one way or the other!
			//$dbColumn->autoIncrement = $column['extra'] == 'auto_increment';
			$dbColumn->isPrimaryKey = $column['constraint_type'] == 'PRIMARY KEY';
			$dbTable->addColumn($dbColumn);
		}
		
		return $dbTable;
	}
	
	/**
     * Returns true if the underlying database is case sensitive, or false otherwise.
     *
     * @return bool
     */
	public function isCaseSensitive() {
		// Pgsql is not case sensitive. Always.
		return false;
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
    public function checkDatabaseExists($dbName) {
		$dbs = $this->getAll("select * from pg_database");
		foreach ($dbs as $db_name)
		{
			if (strtolower($db_name['datname'])==$dbName)
				return true;
		}
		return false;
	}
	
	/**
	 * Sets the sequence to the passed value.
	 *
	 * @param string $seq_name
	 * @param unknown_type $id
	 */
	public function setSequenceId($table_name, $id) {
		$seq_name = $this->getSequenceName($table_name);
		
		$this->exec("SELECT setval('$seq_name', '$id')");
	}
	
	/**
	 * Returns the list of databases available.
	 * 
	 * @return array<string>
	 */
	public function getDatabaseList() {
		throw new DBConnectionException("Not implemented yet");
	}
	
	/**
	 * Returns the underlying type in a db agnostic way, from a string representing the type.
	 * 
	 * For instance, "varchar(255)" or "text" will return "string".
	 * "datetime" will return "datetime", etc...
	 * 
	 * Possible values returned:
	 * - string
	 * - int
	 * - number
	 * - boolean
	 * - timestamp
	 * - datetime
	 * - date
	 * 
	 * @param $type string
	 * @return string
	 */
	public function getUnderlyingType($type) {
		// FIXME: adapt the types to PostgreSQL (this are MySQL type below!!!!)
		$type = strtolower($type);
		$parenPos = strpos($type, "(");
		if ($parenPos !== false) {
			$type = substr($type, 0, $parenPos);
		}
		
		switch ($type) {
			case "int":
			case "tinyint":
			case "smallint":
			case "mediumint":
			case "int":
			case "bigint":
				return "int";
			case "decimal":
			case "float":
			case "double":
			case "real":
				return "number";
			case "bit":
			case "bool":
				return "boolean";
			case "date":
				return "date";
			case "datetime":
				return "datetime";
			case "timestamp":
				return "timestamp";
			default:
				return "string";
		}
	}
	
	/**
     * Creates the database.
     * Of course, a connection must be established for this call to succeed.
     * Please note that you can create a connection without providing a dbname.
     * Please also note that the function does not protect the parameter. You will have to protect
     * it yourself against SQL injection attacks.
     * 
     * @param string $dbName
     */
    public function createDatabase($dbName) {
    	// Overload for Mysql: let's setup the encoding.
    	/*$charset = $this->charset;
    	if (empty($this->charset)) {
    		$charset = "UTF8";
    	}*/
    	$charset = "UTF8";
    	
    	$this->exec("CREATE DATABASE ".$dbName." TEMPLATE = template0 ENCODING = ".$this->quoteSmart($charset));
    	$this->dbname = $dbName;
    	$this->connect();
    }
	
	/**
	 * Returns the table columns.
	 *
	 * @param string $tableName
	 * @return array<array> An array representing the columns for the specified table.
	 */
	public function getTableInfo($tableName) {
		// TODO: EXTEND THIS TO RETRIEVE descriptions (seems to be only available in pg_description table)
		
		$str = "SELECT c.*, tc.constraint_type FROM information_schema.COLUMNS c
				   LEFT JOIN (information_schema.constraint_column_usage co
				        JOIN information_schema.table_constraints tc
				        ON (co.constraint_name = tc.constraint_name AND co.table_name = tc.table_name))
				        ON (c.table_catalog = co.table_catalog AND c.table_name = co.table_name AND c.column_name = co.column_name)
				        WHERE c.table_name = ".$this->quoteSmart($tableName)." AND c.table_catalog = ".$this->quoteSmart($this->dbname)." ;";

		$res = $this->getAll($str);
		
		return $res;
	}
	
	/**
	 * Returns true if the table exists, false if it does not.
	 *
	 * @param string $tableName The name of the table.
	 * @return bool
	 */
	public function isTableExist($tableName) {
		
		
		$str = "SELECT COUNT(1) as cnt FROM information_schema.TABLES WHERE table_name = ".$this->quoteSmart($tableName)." AND table_catalog = ".$this->quoteSmart($this->dbname)." ;";

		$res = $this->getOne($str);
		
		return $res != 0;
	}

	/**
	 * Escape the table name and column name with the special char that depends of database type
	 * 
	 * @param $string string
	 * @return string
	 */
	public function escapeDBItem($string) {
		return '['.$string.']';
	}
}


?>
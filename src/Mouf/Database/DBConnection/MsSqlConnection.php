<?php
namespace Mouf\Database\DBConnection;

use PDO;
use PDOException;

/**
 * A class wrapping a connection to a MsSQL database through PDO, with additional goodies (introspection support)
 * 
 * INFO:
 * Il est possible de faire un export avec compatibilité grace à l'option
 * mysqldump --compatible=mssql 
 * 
 * LIBRARY:
 * http://msdn.microsoft.com/fr-fr/library/ff848799.aspx
 * 
 * INFORMATION_SCHEMA:
 * http://www.mssqltips.com/sqlservertutorial/179/sql-server-informationschema-views/
 *
 * @Component
 * @Logo "mylogo.png"
 */
class MsSqlConnection extends AbstractDBConnection {
	
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
	 * If not set, this will default to UTF8
	 *
	 * @Property
	 * @var string
	 */
	public $charset;
	
	/**
	 * Whether a persistent connection is used or not.
	 * If this application is used on the web, you should choose yes. The database connection
	 * will not be closed when the script stops and will be reused on the next connection.
	 * This will help improve your application's performance. 
	 *
	 * This defaults to "false"
	 * 
	 * @Property
	 * @var boolean
	 */
	public $isPersistentConnection = false;
	
	/**
	 * Returns the DSN for this connection.
	 *
	 * @return string
	 */
	public function getDsn() {
		//osql -U<login id> -P<password> -S<instance name> -i<tsql script file name>
		//ODBC;DRIVER=SQL Server;SERVER=serverName;DATABASE=databaseName;Trusted_Connection=Yes
		//DSN=fred|Database=dave
		$dsn = "DSN=".$this->user."|Database=".$this->dbname;
		if (!empty($this->port)) {
			$dsn .= "|Port=".$this->port;
		}
		$charset = $this->charset;
		if (empty($charset)) {
			$charset = "UTF8";
		}
		$dsn .= "|Charset=".$charset.";";
		
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
		if ($this->isPersistentConnection == true) {
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
	 * Creates a new table in the database.
	 *
	 * @param Table $table The table to create
	 * @param boolean $dropIfExist whether the table should be dropped or not if it exists.
	 */
	public function createTable(Table $table, $dropIfExist = false) {
		$tableName = $table->name;
		$columnsList = $table->columns;
		
		if ($dropIfExist) {
			$sql = "
				IF OBJECT_ID('$tableName', 'U') IS NOT NULL
  					DROP TABLE $tableName";
			$this->exec($sql);
		}
		
		//$sql = "CREATE TABLE $tableName (\n  ID BIGINT NOT NULL AUTO_INCREMENT,";
		$sql = "
			IF OBJECT_ID('$tableName', 'U') IS NULL
  					CREATE TABLE $tableName (\n";
		$first = true;
		$primaryKeyList = array();
		foreach ($columnsList as $column) {
			if (!$first) {
				$sql .= ",\n";
			} else {
				$first = false;
			}
			$sql .= "  ".$column->name." ".$column->type." ";
			if ($column->nullable) {
				$sql .= "NULL";
			} else {
				$sql .= "NOT NULL";
			}
			if ($column->default != null) {
				$sql .= " DEFAULT ".$column->default;
			}
			if ($column->autoIncrement) {
				// #graine : previous // FIXME: comment gérer le #graine ?
				// #pas = 1
				$sql .= " IDENTITY [ ( #graine , #pas ) ";
			}
			if ($column->comment) {
				$sql .= " COMMENT '".mysql_escape_string($column->comment)."'";
			}
			
			if ($column->isPrimaryKey) {
				$primaryKeyList[] = $column->name;
			}
		}
		
		if (!empty($primaryKeyList)) {
			$sql .= ",\n  PRIMARY KEY (".implode(", ", $primaryKeyList).")";
		}
		//$sql .= ",\n  PRIMARY KEY (ID)";
		
		$sql .= ");\n";
		//echo $sql;
		$this->exec($sql);
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
		if ($indexName == null) {
			$indexName = "IDX_".$tableName."_".implode("_", $columnsList); 
		}
		
		// Let's keep the index name short.
		if (strlen($indexName)>40) {
			$newIndexName = substr($indexName, 0, 20);
			$newIndexName .= '_'.md5($indexName);
			$indexName = $newIndexName; 
		}
	
		$sql = "CREATE ";
		$sql .= $isUnique?"UNIQUE ":"";
		$sql .= "INDEX $indexName ON $tableName (".implode(", ", $columnsList).");";
		$this->exec($sql);
	}
	
	/**
	 * Returns Root Sequence Table for $table_name
	 * i.e. : if "man" table inherits "human" table , returns "human" for Root Sequence Table
	 * !! Warning !! Child table must share Mother table's primary key
	 * @param string $table_name
	 * @return string
	 */
	public function findRootSequenceTable($table_name){
		return $table_name;
	}
	
	/**
	 * Returns the parent table (if the table inherits from another table).
	 * For DB systems that do not support inheritence, returns the table name.
	 *
	 * @param string $table_name
	 * @return string
	 */
	public function getParentTable($table_name){
		// No inheritance for Mysql
		return $table_name;
	}
	
	/**
	 * Returns the constraints on table "table_name" and column "column_name" if "column_name"is given
	 * this function returns an array of arrays of the form:
	 * ("table2"=>"name of the constraining table", "col2"=>"name of the constraining column", "col1"=>"name
	 * of the constrained column")
	 * 
	 * FIXME: REFERENCED_TABLE_NAME ne semble pas exister
	 * Remplacé par INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS ?
	 *
	 * @param string $table_name
	 * @param string $column_name
	 * @return unknown
	 */
	public function getConstraintsOnTable($table_name,$column_name=false) {
		if ($column_name)
		{
			$sql = "SELECT DISTINCT column_name as col1, referenced_table_name as table2, referenced_column_name as col2 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='".$this->dbname."' AND TABLE_NAME='$table_name' AND COLUMN_NAME='$column_name'";
		}
		else
		{
			$sql = "SELECT DISTINCT column_name as col1, referenced_table_name as table2, referenced_column_name as col2 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='".$this->dbname."' AND TABLE_NAME='$table_name'";
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
	 * FIXME: REFERENCED_TABLE_NAME ne semble pas exister
	 * Remplacé par INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS ?
	 * 
	 * @param string $table_name
	 * @param string $column_name
	 * @return unknown
	 */
	public function getConstraintsFromTable($table_name,$column_name=false) {
		if ($column_name)
		{
			$sql = "SELECT DISTINCT referenced_column_name as col2, table_name as table1, column_name as col1 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='".$this->dbname."' AND referenced_table_name='$table_name' AND referenced_column_name='$column_name'";
		}
		else
		{
			$sql = "SELECT DISTINCT referenced_column_name as col2, table_name as table1, column_name as col1 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='".$this->dbname."' AND referenced_table_name='$table_name'";
		}

		$result = $this->getAll($sql);

		return $result;
	}
	
	/**
	 * Returns true if the table exists, false if it does not.
	 *
	 * @param string $tableName The name of the table.
	 * @return bool
	 */
	public function isTableExist($tableName) {
		
		
		$str = "SELECT COUNT(1) as cnt FROM information_schema.TABLES WHERE table_name = ".$this->quoteSmart($tableName)." AND table_schema = ".$this->quoteSmart($this->dbname)." ;";

		$res = $this->getOne($str);
		
		return $res != 0;
	}
	
	
	/**
	 * Sets the host (DB server URL) for the connection.
	 *
	 * @param string $host
	 */
	public function setHost($host) {
		$this->host = $host;
	}
	
	/**
	 * Sets the DB port for the connection.
	 *
	 * @param int $host
	 */
	public function setPort($port) {
		$this->port = $port;
	}

	/**
	 * Sets the database name we need to connect to.
	 *
	 * @param string $dbName
	 */
	public function setDbName($dbName) {
		$this->dbname = $dbName;
	}
	
	/**
	 * Sets the user name for the connection.
	 *
	 * @param string $user
	 */
	public function setUser($user) {
		$this->user = $user;
	}
	
	/**
	 * Sets the password for the connection.
	 *
	 * @param string $password
	 */
	public function setPassword($password) {
		$this->password = $password;
	}
	
	/**
	 * Returns the next Id from the sequence.
	 * 
	 * FIXME: LAST_INSERT_ID est remplacé par @@identity ou scope_identity()
	 *
	 * @param string $seq_name The name of the sequence
	 * @param boolean $onDemand If true, if the sequence does not exist, it will be created.
	 * @return int The next value of the sequence
	 */
	public function nextId($seq_name, $onDemand = true) {
		$seqname = $this->getSequenceName($seq_name);
        //do {
        //$repeat = 0;
        try {
        	$nbAff = $this->exec('UPDATE ' . $seqname
                               . ' SET id = scope_identity(id + 1)');
        } catch (PDOException $e) {
        	if ($e->getCode() == '42S02' && $onDemand) {
             // ONDEMAND TABLE CREATION
             $result = $this->createSequence($seq_name);

             return 1;
        	} else {
        		throw $e;	
        	}
        }
	}
	
	public function createSequence($seq_name)
    {
        $seqname = $this->getSequenceName($seq_name);
        $res = $this->exec('CREATE TABLE ' . $seqname
                            . ' (id INTEGER UNSIGNED IDENTITY NOT NULL,'
                            . ' PRIMARY KEY(id))');
        
        // insert yields value 1, nextId call will generate ID 2
        $this->exec("INSERT INTO ${seqname} (id) VALUES (0)");
    }
    
    /**
	 * Returns the table columns.
	 *
	 * @param string $tableName
	 * @return array<array> An array representing the columns for the specified table.
	 */
	public function getTableInfo($tableName) {
		
		$str = "SELECT * FROM information_schema.COLUMNS WHERE table_name = ".$this->quoteSmart($tableName)." AND table_schema = ".$this->quoteSmart($this->dbname)." ;";

		$res = $this->getAll($str);

		// Let's lower case the columns name, in order to get a consistent behaviour with PgSQL
		$arr = array();
		foreach ($res as $nbrow=>$row) {
			foreach ($row as $key=>$value) {
				$arr[$nbrow][strtolower($key)] = $value;	
			}
		}
		
		return $arr;
	}
	
    /**
	 * Returns a table object (Table) from the database.
	 * Throws an exception if the table does not exist. 
	 *
	 * @param string $tableName
	 * @return Table
	 * @throws DBConnectionException
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
			$dbColumn->type = $column['column_type'];
			$dbColumn->nullable = $column['is_nullable'] == 'YES'; 
			$dbColumn->default = $column['column_default'];
			$dbColumn->autoIncrement = $column['extra'] == 'auto_increment';
			$dbColumn->isPrimaryKey = $column['column_key'] == 'PRI';
			$dbColumn->comment = $column['column_comment'];
			$dbTable->addColumn($dbColumn);
		}
		return $dbTable;
	}
	
	/**
	 * Local cache for the case sensitivity.
	 *
	 * @var bool
	 */
	private $caseSensitive = null;
	
	/**
     * Returns true if the underlying database is case sensitive, or false otherwise.
     *
     * @return bool
     */
	public function isCaseSensitive() {
		return $this->caseSensitive;
	}
	
	/**
     * Checks if the database with the given name exists.
     * Returns true if it exists, false otherwise.
     * Of course, a connection must be established for this call to succeed.
     * Please note that you can create a connection without providing a dbname.
     * 
     * DECLARE @dbname nvarchar(128)
     * SET @dbname = N'Senna'
     * 
     * IF (EXISTS (SELECT name 
     * FROM master.dbo.sysdatabases 
     * WHERE ('[' + name + ']' = @dbname 
     * OR name = @dbname)))
     * 
     * @param string $dbName
     * @return bool
     */
    public function checkDatabaseExists($dbName) {
    	// Execute request
    	$str = "
    	DECLARE @dbname nvarchar(128)
		SET @dbname = N'Senna'
		
		IF (EXISTS (SELECT name 
		FROM master.dbo.sysdatabases 
		WHERE ('[$dbName]' = @dbname 
		OR name = @dbname)));";
    	// Get result
    	$res = $this->getAll($str);
    	
    	if(count($res)) {
    		return true;
    	}
    	return false;
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
    	$this->exec("CREATE DATABASE ".$dbName);
    	$this->dbname = $dbName;
    	$this->connect();
    }
    
    /**
	 * Sets the sequence to the passed value.
	 *
	 * @param string $seq_name
	 * @param unknown_type $id
	 */
	public function setSequenceId($table_name, $id) {
		$seq_name = $this->getSequenceName($table_name);
		
		$this->exec("UPDATE $seq_name SET ID='$id'");
	}
	
	/**
	 * Returns the list of databases available.
	 * 
	 * @return array<string>
	 */
	public function getDatabaseList() {
		$str = "
		DECLARE @dbname nvarchar(128)
		SET @dbname = N'Senna'
		
		SELECT name 
		FROM master.dbo.sysdatabases;";
    	// Get result
		$dbs = $this->getAll($str);
		
		$list = array();
		foreach ($dbs as $db_name)
		{
			$list[] = $db_name['name'];
		}
		return $list;
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
	 * @param string $type
	 * @return string
	 */
	public function getUnderlyingType($type) {
		$type = strtolower($type);
		$parenPos = strpos($type, "(");
		if ($parenPos !== false) {
			$type = substr($type, 0, $parenPos);
		}
		$type = trim($type);
		
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
	 * Escape the table name and column name with the special char that depends of database type
	 * 
	 * @param $string string
	 * @return string
	 */
	public function escapeDBItem($string) {
		return ''.$string.'';
	}
	
	/**
	 * Performs the connection to the the database.
	 * This is overloaded because we might want to call 'SET NAMES utf8;'
	 * to set the connection in UTF8. We might have used 
	 * $options[PDO::MYSQL_ATTR_INIT_COMMAND] = "SET NAMES utf8";
	 * or $options[1002] = "SET NAMES utf8";
	 * but this still fails on some computers (especially in Wamp with PHP 5.3.0)
	 *
	 */
	public function connect() {
		parent::connect();
		
		
		$charset = strtolower($this->charset);
		if (empty($charset)) {
			$charset = "utf-8";
		}		
		if ($charset == 'utf8' || $charset == 'utf-8') {
			$this->dbh->exec("SET NAMES utf8;");
		}
	}
	
}


?>
<?php
namespace Mouf\Database\DBConnection; 

use PDO;
use PDOException;

/**
 * A class wrapping a connection to a SQLServer database through PDO/ODBC, with additional goodies (introspection support)
 *
 * @Component
 * @Renderer { "smallLogo":"vendor/mouf/database.dbconnection/icons/database_small.png" }
 */
class MsSqlConnection extends AbstractDBConnection {
	
	/**
	 * The name of the ODBC driver to access SQL Server.
	 * <p>On Windows, you can find that name by:</o>
	 * <ul><li>Starting the "ODBC datasources" application</li>
	 * <li>Going in the "ODBC Drivers" tab</li>
	 * <li>Selecting the appropriate driver in the "Name" column</li></ul>
	 *
	 * <p>Put the text inside {} if it contains spaces.</p>
	 * 
	 * <p>Default value for SQL Server 2012 is {SQL Server Native Client 11.0}</p>
	 *
	 * @var string
	 */
	public $odbcDriver = "{SQL Server Native Client 11.0}";
	
	/**
	 * The server IP address or name.
	 * You can use "(local)" if the server if on your machine 
	 *
	 * @var string
	 */
	public $host = "(local)";
	
	/**
	 * The name of the instance of the database.
	 * 
	 * @var string
	 */
	public $instance = "SQLEXPRESS";
	
	/**
	 * Database user to use when connecting.
	 *
	 * @Property
	 * @var string
	 */
	public $user = "sa";
	
	/**
	 * Password to use when connecting.
	 *
	 * @Property
	 * @var string
	 */
	public $password;
	
	/**
	 * Keep this parameter empty.
	 * You can optionnally set it and it will completely OVERRIDE all parameters used to
	 * create the connection to the database.
	 * 
	 * @var string
	 */
	public $fullOdbcString;

	/**
	 * Whether a persistent connection is used or not.
	 * If this application is used on the web, you should choose yes. The database connection
	 * will not be closed when the script stops and will be reused on the next connection.
	 * This will help improve your application's performance. 
	 *
	 * This defaults to "true"
	 * 
	 * @var boolean
	 */
	public $isPersistentConnection;
	
	/**
	 * Returns the DSN for this connection.
	 *
	 * @return string
	 */
	public function getDsn() {
		if ($this->fullOdbcString) {
			$dsn = $this->fullOdbcString;
		} else {
			$dsn = "odbc:Driver=".$this->odbcDriver.";Server=".$this->host;
			if ($this->instance) {
				$dsn .= "\\".$this->instance;
			}
			$dsn .= ";Database=".$this->dbname.";Uid=".$this->user.";Pwd=".$this->password.";";
		}
		
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
		// No inheritance for Mssql
		return $table_name;
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
			$sql = "SELECT k.table_name table2,
			k.column_name col2,
			ccu.column_name col1
			FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
			LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
			ON k.table_name = c.table_name
			AND k.table_schema = c.table_schema
			AND k.table_catalog = c.table_catalog
			AND k.constraint_catalog = c.constraint_catalog
			AND k.constraint_name = c.constraint_name
			LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
			ON rc.constraint_schema = c.constraint_schema
			AND rc.constraint_catalog = c.constraint_catalog
			AND rc.constraint_name = c.constraint_name
			LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
			ON rc.unique_constraint_schema = ccu.constraint_schema
			AND rc.unique_constraint_catalog = ccu.constraint_catalog
			AND rc.unique_constraint_name = ccu.constraint_name
			WHERE k.constraint_catalog = ".$this->quoteSmart($this->dbname)." AND ccu.table_name=".$this->quoteSmart($table_name)." AND ccu.column_name=".$this->quoteSmart($column_name)." AND c.constraint_type = 'FOREIGN KEY'";
			
		}
		else
		{
			$sql = "SELECT k.table_name table2,
			k.column_name col2,
			ccu.column_name col1
			FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
			LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
			ON k.table_name = c.table_name
			AND k.table_schema = c.table_schema
			AND k.table_catalog = c.table_catalog
			AND k.constraint_catalog = c.constraint_catalog
			AND k.constraint_name = c.constraint_name
			LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
			ON rc.constraint_schema = c.constraint_schema
			AND rc.constraint_catalog = c.constraint_catalog
			AND rc.constraint_name = c.constraint_name
			LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
			ON rc.unique_constraint_schema = ccu.constraint_schema
			AND rc.unique_constraint_catalog = ccu.constraint_catalog
			AND rc.unique_constraint_name = ccu.constraint_name
			WHERE k.constraint_catalog = ".$this->quoteSmart($this->dbname)." AND ccu.table_name=".$this->quoteSmart($table_name)." AND c.constraint_type = 'FOREIGN KEY'";
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
	public function getConstraintsFromTable($table_name,$column_name=false) {
	if ($column_name)
		{
			$sql = "SELECT k.column_name col2,
			ccu.table_name table1,
			ccu.column_name col1,
			k.ordinal_position 'field_position'
			FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
			LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
			ON k.table_name = c.table_name
			AND k.table_schema = c.table_schema
			AND k.table_catalog = c.table_catalog
			AND k.constraint_catalog = c.constraint_catalog
			AND k.constraint_name = c.constraint_name
			LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
			ON rc.constraint_schema = c.constraint_schema
			AND rc.constraint_catalog = c.constraint_catalog
			AND rc.constraint_name = c.constraint_name
			LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
			ON rc.unique_constraint_schema = ccu.constraint_schema
			AND rc.unique_constraint_catalog = ccu.constraint_catalog
			AND rc.unique_constraint_name = ccu.constraint_name
			WHERE k.constraint_catalog = ".$this->quoteSmart($this->dbname)." AND k.table_name=".$this->quoteSmart($table_name)." AND k.column_name=".$this->quoteSmart($column_name)." AND c.constraint_type = 'FOREIGN KEY'";
			
		}
		else
		{
			$sql = "SELECT k.column_name col2,
			ccu.table_name table1,
			ccu.column_name col1,
			k.ordinal_position 'field_position'
			FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
			LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
			ON k.table_name = c.table_name
			AND k.table_schema = c.table_schema
			AND k.table_catalog = c.table_catalog
			AND k.constraint_catalog = c.constraint_catalog
			AND k.constraint_name = c.constraint_name
			LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
			ON rc.constraint_schema = c.constraint_schema
			AND rc.constraint_catalog = c.constraint_catalog
			AND rc.constraint_name = c.constraint_name
			LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
			ON rc.unique_constraint_schema = ccu.constraint_schema
			AND rc.unique_constraint_catalog = ccu.constraint_catalog
			AND rc.unique_constraint_name = ccu.constraint_name
			WHERE k.constraint_catalog = ".$this->quoteSmart($this->dbname)." AND k.table_name=".$this->quoteSmart($table_name)." AND c.constraint_type = 'FOREIGN KEY'";
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
		throw new Exception("Method not implemented yet");
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
		throw new Exception("Method not implemented yet");
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
			$result = $this->getOne("SELECT NEXT VALUE FOR (".$this->quoteSmart($realSeqName).") as nextval");
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
			$type = $column['data_type'];
			if ($type == "nvarchar" || $type == "nchar") {
				$type .= '('.$column['character_maximum_length'].')';
			}
			$dbColumn->type = $type;
			$dbColumn->nullable = $column['is_nullable'] == 'YES'; 
			$dbColumn->default = $column['column_default'];
			$dbColumn->autoIncrement = $column['is_identity'] == 1;
			
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
		$dbs = $this->getAll("select * from sys.databases");
		foreach ($dbs as $db_name)
		{
			if (strtolower($db_name['name'])==$dbName)
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
		
		$this->exec("ALTER SEQUENCE $seq_name RESTART WITH $id");
	}
	
	/**
	 * Returns the list of databases available.
	 * 
	 * @return array<string>
	 */
	public function getDatabaseList() {
		$dbs = $this->getAll("select * from sys.databases");
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
		
		$str = "SELECT c.*, tc.constraint_type,
				COLUMNPROPERTY(object_id(c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY
			 FROM information_schema.COLUMNS c
				   LEFT JOIN (information_schema.constraint_column_usage co
				        JOIN information_schema.table_constraints tc
				        ON (co.constraint_name = tc.constraint_name AND co.table_name = tc.table_name))
				        ON (c.table_catalog = co.table_catalog AND c.table_name = co.table_name AND c.column_name = co.column_name)
				        WHERE c.table_name = ".$this->quoteSmart($tableName)." AND c.table_catalog = ".$this->quoteSmart($this->dbname)." ;";

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
	
	/**
	 * Returns a list of table names.
	 *
	 * 
	 * @param $ignoreSequences boolean: for some databases, sequences are managed with tables. If true, those tables will be ignored. Default is true.
	 * @return array<string>
	 */
	public function getListOfTables($ignoreSequences = true) {
		$str = "SELECT table_name FROM information_schema.TABLES WHERE table_catalog = ".$this->quoteSmart($this->dbname)." AND table_type = 'BASE TABLE';";

		$res = $this->getAll($str);
		$array = array();
		foreach ($res as $table) {
			if (!$ignoreSequences || !$this->isSequenceName($table['table_name'])) {
				$array[] = $table['table_name'];
			}
		}

		return $array;
	}
	
	/**
	 * Protects the string (by adding \ in front of '), or returns the string NULL if value passed is null.
	 * TODO: Migrate to use prepared statements!!
	 *
	 * @param string $in
	 * @return string
	 */
	public function quoteSmart($in) {
		// Note: $this->dbh->quote is completely broken with ODBC driver...
		// So we need to overload this function
		
		return "'".addslashes($in)."'";
	}
}

?>
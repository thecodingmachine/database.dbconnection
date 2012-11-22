<?php
namespace Mouf\Database\DBConnection;

use PDO;

/**
 * This class can be used to proxy requests from one machine to another.
 * It is also pretty usefull when you want to access the database of your application from
 * the Mouf Admin (if you are a Mouf module writter).
 * 
 * @Component
 */
class ConnectionProxy implements ConnectionInterface {
	
	/**
	 * The complete URL to the service that will parse the queries.
	 * If you are using the ConnectionProxy from the Mouf admin to access your application,
	 * this URL is:
	 * 	ROOT_URL."plugins/database/dbconnection/1.0/direct/proxy.php"
	 *
	 * @Property
	 * @Compulsory
	 * @var string
	 */
	public $url;
	
	/**
	 * The name of the DB Connection Mouf instance to be used on the target machine.
	 *
	 * @Property
	 * @Compulsory
	 * @var string
	 */
	public $dbConnectionInstanceName;
	
	/**
	 * Performs the connection to the the database.
	 *
	 */
	public function connect() {
		return $this->proxyCall();
	}
	
	/**
	 * Releases the connection to the database.
	 * Usually, it is not required to close the connection explicitly. The connection will be released at the end of the script.
	 * However, if you have a long running script, at might be a good idea to release the connection as soon as possible so the
	 * connection can be used by other apache processes.
	 *
	 */
	public function close() {
		return $this->proxyCall();
	}
	
	
	/**
	 * Runs the query against the database.
	 *
	 * @param string $query The query to run
	 * @return int the number of line affected
	 */
	public function exec($query) {
		return $this->proxyCall();
	}

	/**
	 * Performs a PDO request
	 *
	 * @param string $query
	 * @param int $from
	 * @param int $limit
	 * @return PDOStatement
	 */
	public function query($query, $from = null, $limit = null) {
		return $this->proxyCall();
	}
	
	/**
	 * Runs the query and returns all lines in an associative table.
	 *
	 * @param string $query
	 * @param int $mode
	 * @param string $classname
	 * @param int $from
	 * @param int $limit
	 * @return array
	 */
	public function getAll($query, $mode = PDO::FETCH_ASSOC, $classname = "stdClass", $from = null, $limit = null) {
		return $this->proxyCall();
	}

	/**
	 * Runs the query and returns the one and only value returned by this query.
	 *
	 * @param string $query
	 * @return mixed
	 */
	public function getOne($query) {
		return $this->proxyCall();
	}

	/**
	 * Protects the string (by adding \ in front of '), or returns the string NULL if value passed is null.
	 * TODO: Migrate to use prepared statements!!
	 * 
	 * @param string $in
	 * @return string
	 */
	public function quoteSmart($in) {
		return $this->proxyCall();
	}

	/**
	 * Returns the next Id from the sequence.
	 *
	 * @param string $seq_name The name of the sequence
	 * @param boolean $onDemand If true, if the sequence does not exist, it will be created.
	 * @return unknown The next value of the sequence
	 */
	public function nextId($seq_name, $onDemand = true) {
		return $this->proxyCall();
	}
	
	/**
	 * Sets the sequence to the passed value.
	 *
	 * @param string $seq_name
	 * @param unknown_type $id
	 */
	public function setSequenceId($table_name, $id) {
		return $this->proxyCall();
	}
	
	/**
	 * Returns Root Sequence Table for $table_name
	 * i.e. : if "man" table inherits "human" table , returns "human" for Root Sequence Table
	 * !! Warning !! Child table must share Mother table's primary key
	 * @param string $table_name
	 * @return string
	 */
	public function findRootSequenceTable($table_name) {
		return $this->proxyCall();
	}
	
	/**
	 * Returns the parent table (if the table inherits from another table).
	 * For DB systems that do not support inheritence, returns the table name.
	 *
	 * @param string $table_name
	 * @return string
	 */
	public function getParentTable($table_name) {
		return $this->proxyCall();
	}
	
	/**
	 * Returns an array of columns that are declared to be primary keys for this table.
	 *
	 * @param string $table_name the table name
	 * @return array<Column> an array of the primary key columns of the table
	 */
	public function getPrimaryKey($table_name) {
		return $this->proxyCall();
	}

	/**
	 * Returns the table columns.
	 *
	 * @param string $tableName
	 * @return array<array> An array representing the columns for the specified table.
	 */
	public function getTableInfo($tableName) {
		return $this->proxyCall();
	}
	
	/**
	 * Returns a list of table names.
	 *
	 * @param $ignoreSequences boolean: for some databases, sequences are managed with tables. If true, those tables will be ignored. Default is true.
	 * @return array<string>
	 */
	public function getListOfTables($ignoreSequences = true) {
		return $this->proxyCall();
	}
	
	/**
	 * Returns true if the table exists, false if it does not.
	 *
	 * @param string $tableName The name of the table.
	 * @return bool
	 */
	public function isTableExist($tableName) {
		return $this->proxyCall();
	}
		
	/**
	 * Returns a table object (Table) from the database. 
	 *
	 * @param string $tableName
	 * @return Table
	 */
	public function getTableFromDbModel($tableName) {
		return $this->proxyCall();
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
		return $this->proxyCall();
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
		return $this->proxyCall();
	}

	public function getInsertId($table_name,$pkey_field_name) {
		return $this->proxyCall();
	}

	/**
	 * Returns true if the table exists.
	 * Otherwise, tries to find a close match and returns a table of the closest matches.
	 *
	 * Returns null in case of error.
	 * This function is used in case an exception is thrown to try to help the user find which table he wants.
	 *
	 * @param string $table_name name of the table to find
	 */
	function checkTableExist($table_name) {
		return $this->proxyCall();
	}
	
	/**
	 * Returns true if the column in the given table exists.
	 * Otherwise, tries to find a close match and returns an array of the closest matches.
	 *
	 * Returns null in case of error.
	 * This function is used in case an exception is thrown to try to help the user find which column he wants.
	 *
	 * @param string $table_name
	 * @param string $column_name
	 * @return true|array<string>
	 */
	function checkColumnExist($table_name, $column_name) {
		return $this->proxyCall();
	}
		
	/**
	 * Returns, depending on the database system used and file system used the string passed
	 * in parameter in lowercase or in the same case.
	 * For instance, with a PgSQL database, you will always get a lowercase string.
	 * On MySQL, it will depend the system used. By default, on Windows, it should return a lowercase string
	 * while on Linux, it will return the same string.
	 * The database setting is retrieved only once and stored in session to avoid unnecessary database calls.
	 *
	 * TODO: change the session mecanism so we can use 2 different databases. Right now, they should have the same
	 * case sensitivity settings, which is not good.
	 *
	 */
	function toStandardcase($string) {
		return $this->proxyCall();
	}
	
	/**
	 * Returns, depending on the database system used and file system used the string passed 
	 * in parameter in lowercase or in the same case.
	 * This lowercasing mecanism is used for columns.
	 * If a column name is case insensitive, this will return the column name in lowercase.
	 * 
	 * Note: in the current implementation, both MySQL and PostgreSQL are case insensitive.
	 * PostgreSQL is case sensitive if a column is quoted, BUT TDBM does not quote columns.
	 * 
	 */
	function toStandardcaseColumn($string) {
		return $this->proxyCall();
	}

	/**
	 * Begins a transaction. You must use commit or rollback to end the transaction.
	 * By default, if the scripts finishes and none of commit and rollback have been called,
	 * the transaction will be rolled-back.
	 *
	 * @return bool true on success, false on failure.
	 */
	public function beginTransaction() {
		return $this->proxyCall();
	}
	
	/**
	 * Commits the transaction that has been started with beginTransaction.
	 *
	 * @return bool true on success, false on failure.
	 */
	public function commit() {
		return $this->proxyCall();
	}
	
	/**
	 * Rolls-back the transaction that has been started with beginTransaction.
	 *
	 * @return bool true on success, false on failure.
	 */
	public function rollback() {
		return $this->proxyCall();
	}
	
	/**
	 * Returns the column type of the column $column from table $table
	 * If the column does not exist, returns null.
	 * If the table does not exist, throws a TDBM_Exception
	 *
	 * @param string $table
	 * @param string $column
	 */
	public function getColumnType($table, $column) {
		return $this->proxyCall();
	}
	
	/**
	 * Checks whether the $value passed is compatible with the SQL $type passed.
	 * For instance checkType(4, 'INTEGER') will return true.
	 * checkType('toto', 'INTEGER') will return false.
	 *
	 * @param unknown_type $value
	 * @param string $type
	 * @return boolean
	 */
	public function checkType($value, $type) {
		return $this->proxyCall();
	}
	
	/**
	 * Creates a new table in the database.
	 *
	 * @param Table $table The table to create
	 * @param boolean $dropIfExist whether the table should be dropped or not if it exists.
	 */
	public function createTable(Table $table, $dropIfExist = false) {
		return $this->proxyCall();
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
		return $this->proxyCall();
	}
	
	/**
	 * Returns the sequence name (code from Pear DB, thanks to the Pear DB team).
	 *
	 * @param unknown_type $sqn
	 * @return unknown
	 */
	public function getSequenceName($sqn) {
		return $this->proxyCall();
	}
    
    /**
     * Returns true of name passed in parameter matches the sequence name pattern.
     * 
     * @param string $sqn
     * @return boolean
     */
    public function isSequenceName($sqn) {
		return $this->proxyCall();
	}
	
    /**
     * Creates a sequence with the name specified.
     * Note: The name is transformed be the getSequenceName method.
     * By default, if "mytable" is passed, the name of the sequence will be "mytable_pk_seq".
     *
     * @param string $seq_name
     */
    public function createSequence($seq_name) {
		return $this->proxyCall();
	}
	
    /**
     * Returns true if the underlying database is case sensitive, or false otherwise.
     *
     * @return bool
     */
	public function isCaseSensitive() {
		return $this->proxyCall();
	}
	
	/**
	 * True if there is an active transaction (started with beginTransaction(), false otherwise).
	 * Note: this flag might be false in MySQL. If a DDL query is issued (like "DROP TABLE test"), the current transaction
	 * will be ended, but the flag will not be set to false).
	 * 
	 * @return bool
	 */
    public function hasActiveTransaction() {
		return $this->proxyCall();
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
		return $this->proxyCall();
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
		return $this->proxyCall();
	}

    /**
     * Drops the database.
     * Of course, a connection must be established for this call to succeed.
     * Please note that you can create a connection without providing a dbname.
     * Please also note that the function does not protect the parameter. You will have to protect
     * it yourself against SQL injection attacks.
     * 
     * @param string $dbName
     */
    public function dropDatabase($dbName) {
		return $this->proxyCall();
	}
    
    /**
	 * Executes the given SQL file.
	 * If $on_error_continue == true, continues if an error is encountered.
	 * Otherwise, stops.
	 * 
	 * Returns true on success, false if errors have been encountered (even non fatal errors).
	 *
	 * @param string $file The SQL filename
	 * @param bool $on_error_continue
	 */
	public function executeSqlFile($file, $on_error_continue = true) {
		return $this->proxyCall();
	}
	
	/**
	 * Returns the list of databases available.
	 * 
	 * @return array<string>
	 */
	public function getDatabaseList() {
		return $this->proxyCall();
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
		return $this->proxyCall();
	}

	/**
	 * Escape the table name and column name with the special char that depends of database type
	 * 
	 * @param string $string
	 * @return string
	 */
	public function escapeDBItem($string) {
		return $this->proxyCall();
	}
	
	/**
	 * Performs a call to the proxy.
	 * The function uses the debug_backtrace function to learn what is the caller
	 * and what where the parameters passed.
	 */
	private function proxyCall() {
		$debugBackTrace = debug_backtrace();
		$callingFunctionTrace = $debugBackTrace[1];
		$methodName = $callingFunctionTrace['function'];
		$args = $callingFunctionTrace['args'];
		
		$postArray = array("instance"=>$this->dbConnectionInstanceName, "method"=>$methodName, "args"=>serialize($args));
		
		$response = $this->performRequest($this->url, $postArray);
		$obj = unserialize($response);
		
		if ($obj === false) {
			throw new Exception("Unable to unserialize message:\n".$response."\n<br/>URL in error: <a href='".plainstring_to_htmlprotected($url)."'>".plainstring_to_htmlprotected($url)."</a>");
		}
		
		return $obj;
	}
	
	private static function performRequest($url, $post = array()) {
		// preparation de l'envoi
		$ch = curl_init();
				
		curl_setopt($ch, CURLOPT_URL, $url);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		
		if($post) {
			curl_setopt($ch, CURLOPT_POST, true);
			curl_setopt($ch, CURLOPT_POSTFIELDS, $post);
		} else {
			curl_setopt($ch, CURLOPT_POST, false);
		}
		$response = curl_exec($ch );
		
		if( curl_error($ch) ) { 
			throw new DBConnectionException("An error occured: ".curl_error($ch));
		}
		curl_close( $ch );
		
		return $response;
	}
	
	/**
	 * Returns a proxy to the local application database.
	 * This is used by admin modules to access the database of the Mouf application.
	 * 
	 * @param string $dbConnectionInstanceName
	 */
	public static function getLocalProxy($dbConnectionInstanceName = "dbConnection") {
		$dbProxy = new ConnectionProxy();
		$dbProxy->dbConnectionInstanceName = $dbConnectionInstanceName;
		$dbProxy->url = MoufReflectionProxy::getLocalUrlToProject()."plugins/database/dbconnection/1.0/direct/proxy.php";
		return $dbProxy;
	}
}
<?php

/**
 * This class can be used to cache the requests describing the database model.
 * 
 * @Component
 */
class DB_CachedConnection implements DB_ConnectionInterface {
	
	private static $CACHE_KEY = "__DB_CONNECTION__";
	
	/**
	 * The DB Connection to be cached.
	 *
	 * @Property
	 * @Compulsory
	 * @var DB_ConnectionInterface
	 */
	public $dbConnection;
	
	/**
	 * The cache service to be used to cache the database model.
	 *
	 * @Property
	 * @Compulsory
	 * @var CacheInterface
	 */
	public $cacheService;
	
	/**
	 * The content of the cache variable.
	 *
	 * @var array<string, mixed>
	 */
	private $cache;
	
	/**
	 * Performs the connection to the the database.
	 *
	 */
	public function connect() {
		$this->dbConnection->connect();
	}
	
	/**
	 * Releases the connection to the database.
	 * Usually, it is not required to close the connection explicitly. The connection will be released at the end of the script.
	 * However, if you have a long running script, at might be a good idea to release the connection as soon as possible so the
	 * connection can be used by other apache processes.
	 *
	 */
	public function close() {
		$this->dbConnection->close();
	}
	
	/**
	 * Loads the cache and stores it (to be reused in this instance).
	 * Note: the cache is not returned. It is stored in the $cache instance variable.
	 */
	private function loadCache() {
		if ($this->cache == null) {
			$this->cache = $this->cacheService->get(self::$CACHE_KEY);
		}
	}
	
	/**
	 * Saves the cache.
	 *
	 */
	private function saveCache() {
		$this->cacheService->set(self::$CACHE_KEY, $this->cache);
	}
	
	/**
	 * Runs the query against the database.
	 *
	 * @param string $query The query to run
	 * @return int the number of line affected
	 */
	public function exec($query) {
		return $this->dbConnection->exec($query);
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
		return $this->dbConnection->query($query, $from, $limit);
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
		return $this->dbConnection->getAll($query, $mode, $classname, $from, $limit);
	}

	/**
	 * Runs the query and returns the one and only value returned by this query.
	 *
	 * @param string $query
	 * @return mixed
	 */
	public function getOne($query) {
		return $this->dbConnection->getOne($query);
	}

	/**
	 * Protects the string (by adding \ in front of '.
	 * TODO: Migrate to use prepared statements!!
	 * 
	 * @param string $in
	 * @return string
	 */
	public function quoteSmart($in) {
		return $this->dbConnection->quoteSmart($in);
	}

	/**
	 * Returns the next Id from the sequence.
	 *
	 * @param string $seq_name The name of the sequence
	 * @param boolean $onDemand If true, if the sequence does not exist, it will be created.
	 * @return unknown The next value of the sequence
	 */
	public function nextId($seq_name, $onDemand = true) {
		return $this->dbConnection->nextId($seq_name, $onDemand);
	}
	
	/**
	 * Sets the sequence to the passed value.
	 *
	 * @param string $seq_name
	 * @param unknown_type $id
	 */
	public function setSequenceId($table_name, $id) {
		return $this->dbConnection->setSequenceId($table_name, $id);
	}
	
	/**
	 * Returns Root Sequence Table for $table_name
	 * i.e. : if "man" table inherits "human" table , returns "human" for Root Sequence Table
	 * !! Warning !! Child table must share Mother table's primary key
	 * @param string $table_name
	 * @return string
	 */
	public function findRootSequenceTable($table_name){
		$this->loadCache();
		if (!isset($this->cache["rootsequencetable"]) || !isset($this->cache["rootsequencetable"][$table_name])) {
			$this->cache["rootsequencetable"][$table_name] = $this->dbConnection->findRootSequenceTable($table_name);
			$this->saveCache();
		}
		return $this->cache["rootsequencetable"][$table_name];
	}
	
	/**
	 * Returns the parent table (if the table inherits from another table).
	 * For DB systems that do not support inheritence, returns the table name.
	 *
	 * @param string $table_name
	 * @return string
	 */
	public function getParentTable($table_name) {
		$this->loadCache();
		if (!isset($this->cache["parenttables"]) || !isset($this->cache["parenttables"][$table_name])) {
			$this->cache["parenttables"][$table_name] = $this->dbConnection->getParentTable($table_name);
			$this->saveCache();
		}
		return $this->cache["parenttables"][$table_name];
	}
	
	/**
	 * Returns an array of columns that are declared to be primary keys for this table.
	 *
	 * @param string $table_name the table name
	 * @return array<DB_Column> an array of the primary key columns of the table
	 */
	public function getPrimaryKey($table_name) {
		$this->loadCache();
		if (!isset($this->cache["primarykeys"]) || !isset($this->cache["primarykeys"][$table_name])) {
			$this->cache["primarykeys"][$table_name] = $this->dbConnection->getPrimaryKey($table_name);
			$this->saveCache();
		}
		return $this->cache["primarykeys"][$table_name];
	}
	
	/**
	 * Returns the table columns.
	 *
	 * @param string $tableName
	 * @return array<array> An array representing the columns for the specified table.
	 */
	public function getTableInfo($tableName) {
		$this->loadCache();
		if (!isset($this->cache["tableinfo"]) || !isset($this->cache["tableinfo"][$tableName])) {
			$this->cache["tableinfo"][$tableName] = $this->dbConnection->getTableInfo($tableName);
			$this->saveCache();
		}
		return $this->cache["tableinfo"][$tableName];
	}
		
	/**
	 * Returns a list of table names.
	 *
	 * @param $ignoreSequences boolean: for some databases, sequences are managed with tables. If true, those tables will be ignored. Default is true.
	 * @return array<string>
	 */
	public function getListOfTables($ignoreSequences = true) {
		$this->loadCache();
		if (!isset($this->cache["listoftables"])) {
			$this->cache["listoftables"] = $this->dbConnection->getListOfTables($ignoreSequences);
			$this->saveCache();
		}
		return $this->cache["listoftables"];
	}
	
	/**
	 * Returns true if the table exists, false if it does not.
	 *
	 * @param string $tableName The name of the table.
	 * @return bool
	 */
	public function isTableExist($tableName) {
		$this->loadCache();
		if (!isset($this->cache["isTableExist"]) || !isset($this->cache["isTableExist"][$tableName])) {
			$this->cache["isTableExist"][$tableName] = $this->dbConnection->isTableExist($tableName);
			$this->saveCache();
		}
		return $this->cache["isTableExist"][$tableName];
	}
		
	/**
	 * Returns a table object (DB_Table) from the database. 
	 *
	 * @param string $tableName
	 * @return DB_Table
	 */
	public function getTableFromDbModel($tableName) {
		$this->loadCache();
		if (!isset($this->cache["getTableFromDbModel"]) || !isset($this->cache["getTableFromDbModel"][$tableName])) {
			$this->cache["getTableFromDbModel"][$tableName] = $this->dbConnection->getTableFromDbModel($tableName);
			$this->saveCache();
		}
		return $this->cache["getTableFromDbModel"][$tableName];
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
		$this->loadCache();
		if (!isset($this->cache["getConstraintsOnTable"]) || !isset($this->cache["getConstraintsOnTable"][$table_name]) || !isset($this->cache["getConstraintsOnTable"][$table_name][$column_name])) {
			$this->cache["getConstraintsOnTable"][$table_name][$column_name] = $this->dbConnection->getConstraintsOnTable($table_name, $column_name);
			$this->saveCache();
		}
		return $this->cache["getConstraintsOnTable"][$table_name][$column_name];
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
		$this->loadCache();
		if (!isset($this->cache["getConstraintsFromTable"]) || !isset($this->cache["getConstraintsFromTable"][$table_name]) || !isset($this->cache["getConstraintsFromTable"][$table_name][$column_name])) {
			$this->cache["getConstraintsFromTable"][$table_name][$column_name] = $this->dbConnection->getConstraintsFromTable($table_name, $column_name);
			$this->saveCache();
		}
		return $this->cache["getConstraintsFromTable"][$table_name][$column_name];
	}

	public function getInsertId($table_name,$pkey_field_name) {
		return $this->dbConnection->getInsertId($table_name, $pkey_field_name);
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
		return $this->dbConnection->checkTableExist($table_name);
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
		return $this->dbConnection->checkColumnExist($table_name, $column_name);
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
		return $this->dbConnection->toStandardcase($string);
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
		return $this->dbConnection->toStandardcaseColumn($string);
	}

	/**
	 * Begins a transaction. You must use commit or rollback to end the transaction.
	 * By default, if the scripts finishes and none of commit and rollback have been called,
	 * the transaction will be rolled-back.
	 *
	 * @return bool true on success, false on failure.
	 */
	public function beginTransaction() {
		return $this->dbConnection->beginTransaction();
	}
	
	/**
	 * Commits the transaction that has been started with beginTransaction.
	 *
	 * @return bool true on success, false on failure.
	 */
	public function commit() {
		return $this->dbConnection->commit();
	}
	
	/**
	 * Rolls-back the transaction that has been started with beginTransaction.
	 *
	 * @return bool true on success, false on failure.
	 */
	public function rollback() {
		return $this->dbConnection->rollback();
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
		// TODO: we should use the getTableWithModel function and get info from here. That would restrict the size of the cache.
		$this->loadCache();
		if (!isset($this->cache["getColumnType"][$table][$column])) {
			$this->cache["getColumnType"][$table][$column] = $this->dbConnection->getColumnType($table, $column);
			$this->saveCache();
		}
		return $this->cache["getColumnType"][$table][$column];
	}
	
	/**
	 * Checks whether the $value passed is compatible with the SQL $type passed.
	 * For instance checkType(4, 'INTEGER') will return true.
	 * checkType('toto', 'INTEGER') will return false.
	 *
	 * @param mixed $value
	 * @param string $type
	 * @return boolean
	 */
	public function checkType($value, $type) {
		return $this->dbConnection->checkType($value, $type);
	}
	
	/**
	 * Creates a new table in the database.
	 *
	 * @param DB_Table $table The table to create
	 * @param boolean $dropIfExist whether the table should be dropped or not if it exists.
	 */
	public function createTable(DB_Table $table, $dropIfExist = false) {
		return $this->dbConnection->createTable($table, $dropIfExist);
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
		return $this->dbConnection->createIndex($tableName, $columnsList, $isUnique, $indexName);
	}
	
	/**
	 * Returns the sequence name (code from Pear DB, thanks to the Pear DB team).
	 *
	 * @param unknown_type $sqn
	 * @return unknown
	 */
	public function getSequenceName($sqn) {
		return $this->dbConnection->getSequenceName($sqn);
	}
    
    /**
     * Returns true of name passed in parameter matches the sequence name pattern.
     * 
     * @param $sqn
     * @return boolean
     */
    public function isSequenceName($sqn) {
    	return $this->dbConnection->isSequenceName($sqn);
    }
	
    /**
     * Creates a sequence with the name specified.
     * Note: The name is transformed be the getSequenceName method.
     * By default, if "mytable" is passed, the name of the sequence will be "mytable_pk_seq".
     *
     * @param string $seq_name
     */
    public function createSequence($seq_name) {
    	return $this->dbConnection->createSequence($seq_name);
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
    
    /**
     * Returns true if the underlying database is case sensitive, or false otherwise.
     *
     * @return bool
     */
	public function isCaseSensitive() {
		$this->loadCache();
		if (!isset($this->cache["toStandardcase"])) {
			$this->cache["toStandardcase"] = $this->dbConnection->isCaseSensitive();
			$this->saveCache();
		}
		return $this->cache["toStandardcase"];
	}
	
	/**
	 * True if there is an active transaction (started with beginTransaction(), false otherwise).
	 * Note: this flag might be false in MySQL. If a DDL query is issued (like "DROP TABLE test"), the current transaction
	 * will be ended, but the flag will not be set to false).
	 * 
	 * @return bool
	 */
    public function hasActiveTransaction() {
    	return $this->dbConnection->hasActiveTransaction();
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
		return $this->dbConnection->checkDatabaseExists($dbName);
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
    	$this->dbConnection->createDatabase($dbName);
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
    	$this->dbConnection->dropDatabase($dbName);
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
		$this->dbConnection->executeSqlFile($file, $on_error_continue);
	}
	
	/**
	 * Returns the list of databases available.
	 * 
	 * @return array<string>
	 */
	public function getDatabaseList() {
		$this->dbConnection->getDatabaseList();
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
		return $this->dbConnection->getUnderlyingType($type);
	}
	
	/**
	 * Escape the table name and column name with the special char that depends of database type
	 * 
	 * @param $string string
	 * @return string
	 */
	public function escapeDBItem($string) {
		return $this->dbConnection->escapeDBItem($string);
	}
}
?>
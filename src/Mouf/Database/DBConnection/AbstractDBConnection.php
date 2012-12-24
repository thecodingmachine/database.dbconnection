<?php
namespace Mouf\Database\DBConnection; 

use PDO;
use Exception;
use PDOException;
use Mouf\Utils\Log\LogInterface;

/**
 * An abstract class representing wrapping a connection to PDO with additional goodies (introspection support)
 *
 */
abstract class AbstractDBConnection implements ConnectionSettingsInterface, ConnectionInterface {

	/**
	 * The database handler. This is an object whose type is PDO.
	 *
	 * @var PDO
	 */
	protected $dbh;

	/**
	 * The name for the database instance to connect to.
	 *
	 * @Property
	 * @Compulsory
	 * @var string
	 */
	public $dbname;

	/**
	 * The logger to use, if any.
	 *
	 * @Property
	 * @var LogInterface
	 */
	public $log;

	/**
	 * Strictly positive if there is an active transaction (started with beginTransaction(), false otherwise).
	 * The counter is incremented each time a call to beginTransaction is performed.
	 * Note: this flag might be false in MySQL. If a DDL query is issued (like "DROP TABLE test"), the current transaction
	 * will be ended, but the flag will not be set to 0).
	 *
	 * @var int
	 */
	protected $transactionLevel = 0;

	/*public $db;
	 public $dsn;
	 public $options;
	 private $commitOnQuit = true;
	 private $autoCommit = true;*/

	public function __construct() {
		//$this->dsn = $dsn;
		/*if (!isset($options['seqname_format']))
		 $options['seqname_format'] = '%s_pk_seq';
		 if (!isset($options['persistent']))
		 $options['persistent'] = 'TRUE';

		 // By default, charset for the connection will be UTF-8.
		 if (!isset($dsn['charset']))
		 $dsn['charset']="UTF8";

		 $this->dsn = $dsn;
		 $this->options = $options;
		 $this->db =& DB::connect($dsn, $options);
		 $this->checkError($this->db);

		 // In the case of MySQL UTF8, there is an additional command to run!
		 if ($this->dsn["phptype"]=='mysql') {
			$charset = strtolower($dsn['charset']);
			if ($charset == 'utf8' || $charset == 'utf-8')
			$this->query("SET NAMES 'utf8'");
			}*/

	}

	/**
	 * Performs the connection to the the database.
	 *
	 */
	public function connect() {
		$this->dbh = new PDO($this->getDsn(), $this->getUserName(), $this->getPassword(), $this->getOptions());
	}
	
	/**
	 * Releases the connection to the database.
	 * Usually, it is not required to close the connection explicitly. The connection will be released at the end of the script.
	 * However, if you have a long running script, at might be a good idea to release the connection as soon as possible so the 
	 * connection can be used by other apache processes. 
	 * 
	 */
	public function close() {
		$this->dbh = null;
	}


	/**
	 * Runs the query against the database.
	 *
	 * @param string $query The query to run
	 * @return int the number of line affected
	 */
	public function exec($query) {
		if ($this->dbh == null) {
			$this->connect();
		}

		$res = $this->dbh->exec($query);
		return $res;
	}

	/**
	 * Performs a PDO request.
	 * Note: the responsibility of closing the statement is left to the caller.
	 *
	 * @param string $query
	 * @param int $from
	 * @param int $limit
	 * @return PDOStatement
	 */
	public function query($query, $from = null, $limit = null) {
		if ($this->dbh == null) {
			$this->connect();
		}
		
		$queryStr = $query;
		$queryStr .= $this->getFromLimitString($from, $limit);

		try {
			$res = $this->dbh->query($queryStr);
		} catch (\PDOException $e) {
			$this->error("An error occured while playing this request: ".$queryStr);
			throw $e;
		}

		return $res;
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
		if($classname==null && $mode==PDO::FETCH_CLASS) $classname = "stdClass";
		if ($this->dbh == null) {
			$this->connect();
		}
		
		$query .= $this->getFromLimitString($from, $limit);
		
		$stmt = $this->dbh->query($query);
		if($mode==PDO::FETCH_CLASS){
			$stmt->setFetchMode(PDO::FETCH_CLASS,$classname);
		}else {
			$stmt->setFetchMode($mode);
		}
		$arrValues = $stmt->fetchAll();
		$stmt->closeCursor();

		return $arrValues;
	}

	private function getFromLimitString($from = null, $limit = null) {
		if ($limit !== null) {
			$limitInt = (int)$limit;
			$queryStr = " LIMIT ".$limitInt;
				
			if ($from !== null) {
				$fromInt = (int)$from;
				$queryStr .= " OFFSET ".$fromInt;
			}
			return $queryStr;
		}
		else
		{
			return "";
		}
	}
	
	/**
	 * Runs the query and returns the one and only value returned by this query.
	 *
	 * @param string $query
	 * @return mixed
	 */
	public function getOne($query) {
		if ($this->dbh == null) {
			$this->connect();
		}

		$stmt = $this->dbh->query($query);
		$value = $stmt->fetchColumn();
		$stmt->closeCursor();

		return $value;
	}

	/**
	 * Protects the string (by adding \ in front of '), or returns the string NULL if value passed is null.
	 * TODO: Migrate to use prepared statements!!
	 *
	 * @param string $in
	 * @return string
	 */
	public function quoteSmart($in) {
		if ($this->dbh == null) {
			$this->connect();
		}
		if ($in === null) {
			return 'NULL';
		}
		
		return $this->dbh->quote($in);
	}

	/**
	 * Returns the next Id from the sequence.
	 *
	 * @param string $seq_name The name of the sequence
	 * @param boolean $onDemand If true, if the sequence does not exist, it will be created.
	 * @return unknown The next value of the sequence
	 */
	//abstract public function nextId($seq_name, $onDemand = true);

	/*public function nextId($seq_name, $onDemand = true) {
		$id = $this->db->nextId($seq_name, $onDemand);
		$this->checkError($id, 'Error while querying peardb sequence name '.$seq_name.'\n'.
		"Possible symptom: the sequence is named $seq_name and your database user (".$this->getUserName().") does not have the rights to create it.");
		return $id;
		}*/


	/**
	 * Returns Root Sequence Table for $table_name
	 * i.e. : if "man" table inherits "human" table , returns "human" for Root Sequence Table
	 * !! Warning !! Child table must share Mother table's primary key
	 * @param unknown_type $table_name
	 */
	//abstract protected function findRootSequenceTable($table_name);

	/**
	 * Returns parent table according to child parent's primary key's contraint
	 * !! warning !! : we assume no DBMS can add a constraint from a primary key to a targeted column that is not a primary key.
	 * TODO : à compléter
	 * @param unknown_type $table_name
	 */
	private function getParentTableByPrimaryForeignKey($table_name){
		$primary_key = $this->getPrimaryKey($table_name);
		$parent_table = null;
		// Primary Keys made of several columns are not handled
		if (count($primary_key)>1) {
			throw new TDBM_Exception('Unable to handle multi-column primary keys. <br />\n
			Can\'t find Root Sequence Table for table '.$table_name );
		}elseif (count($primary_key)==0){
			throw new TDBM_Exception('No primary key for table '.$table_name );
		}
		$primary_key = $primary_key[0];
		$constraint_array = $this->getConstraintsFromTable($table_name);
		foreach ($constraint_array as $constraint){
			if ($constraint['col1']==$primary_key) {
				$parent_table = $constraint['table_2'];
				break;
			}
		}
		return $parent_table;
	}

	/**
	 * Returns an array of columns that are declared to be primary keys for this table.
	 *
	 * @param string $table_name the table name
	 * @return array<Column> an array of the primary key columns of the table
	 */
	public function getPrimaryKey($table_name) {

		$table = $this->getTableFromDbModel($table_name);
		return $table->getPrimaryKeys();
	}

	/**
	 * Returns a list of table names.
	 *
	 * 
	 * @param $ignoreSequences boolean: for some databases, sequences are managed with tables. If true, those tables will be ignored. Default is true.
	 * @return array<string>
	 */
	public function getListOfTables($ignoreSequences = true) {
		$str = "SELECT table_name FROM information_schema.TABLES WHERE table_schema = ".$this->quoteSmart($this->dbname)." AND table_type = 'BASE TABLE';";

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
	 * Returns a table object (Table) from the database.
	 *
	 * @param string $tableName
	 * @return Table
	 */
	//abstract public function getTableFromDbModel($tableName);

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
	//abstract public function getConstraintsOnTable($table_name,$column_name=false);

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
	//abstract public function getConstraintsFromTable($table_name,$column_name=false);

	/**
	 * Returns a table of rows with structure:
	 * ("constraining_column" => XXX, "constrained_column" => XXX)
	 *
	 * @param unknown_type $constraining_table The constraining table
	 * @param unknown_type $constrained_table The constrained table
	 * @return unknown
	 */
	/*	function getConstraintsBetweenTable($constraining_table,$constrained_table) {
		if ($this->dsn["phptype"]=='pgsql')
		{
		$sql = "SELECT c1.attname as constraining_column, c2.attname as constrained_column FROM
		pg_attribute c2 JOIN pg_class t2 JOIN
		(pg_constraint con JOIN
		(pg_class t1 JOIN pg_attribute c1 ON t1.oid = c1.attrelid)
		ON con.conrelid = t1.oid AND con.conkey[1]=c1.attnum)
		ON t2.oid = con.confrelid ON c2.attrelid = t2.oid AND con.confkey[1]=c2.attnum
		WHERE t2.relname='$constrained_table' AND t1.relname='$constraining_table'";
			
		$result = $this->db->getAll($sql,null,DB_FETCHMODE_ASSOC);
		$this->checkError($result, $sql);
		return $result;
		}
		throw new TDBM_Exception('Unable to retrieve Constraints for database type '.$this->dsn['phptype']);
		}

		function findPivotTable($table1,$table2) {
		if ($this->dsn["phptype"]=='pgsql')
		{
		$sql = "SELECT col1.attname AS col1, colpivot1.attname AS colpivot1, pivottable.relname AS pivottable, colpivot2.attname AS colpivot2, col2.attname AS col2 FROM
		(((((((pg_class pivottable JOIN pg_constraint con12 ON pivottable.oid = con12.conrelid)
		JOIN pg_constraint con23 ON pivottable.oid = con23.conrelid)
		JOIN pg_class t1 ON t1.oid = con12.confrelid)
		JOIN pg_class t2 ON t2.oid = con23.confrelid)
		JOIN pg_attribute col1 ON t1.oid = col1.attrelid AND con12.confkey[1]=col1.attnum)
		JOIN pg_attribute col2 ON t2.oid = col2.attrelid AND con23.confkey[1]=col2.attnum)
		JOIN pg_attribute colpivot1 ON pivottable.oid = colpivot1.attrelid AND con12.conkey[1]=colpivot1.attnum)
		JOIN pg_attribute colpivot2 ON pivottable.oid = colpivot2.attrelid AND con23.conkey[1]=colpivot2.attnum
		WHERE t2.relname='$table1' AND t1.relname='$table2'";
			
		$result = $this->db->getAll($sql,null,DB_FETCHMODE_ASSOC);
		$this->checkError($result, $sql);
		return $result;
		}
		throw new TDBM_Exception('Unable to retrieve Constraints for database type '.$this->dsn['phptype']);
		}*/

	public function getInsertId($table_name,$pkey_field_name) {
		$sql="SELECT max($pkey_field_name) AS id FROM $table_name";
		//echo $sql;
		$result=$this->getOne($sql);

		return $result;
	}



	/**
	 * Returns true if the table exists.
	 * Otherwise, tries to find a close match and returns a table of the closest matches.
	 *
	 * Returns null in case of error.
	 * This function is used in case an exception is thrown to try to help the user find which table he wants.
	 *
	 * @param string $table_name name of the table to find
	 * @return mixed
	 */
	function checkTableExist($table_name) {
		if ($this->isTableExist($table_name)) {
			return true;
		}

		// If the table does not exist, let's try to find a close match in the name.
		$data = $this->getListOfTables();

		// Let's compute the lenvenstein distance and keep the smallest one in $smallest.
		$smallest = 99999999;
		$distance_table = array();

		foreach ($data as $current_table) {
			$distance = levenshtein($table_name, $current_table);
			$distance_table[$current_table]=$distance;
			if ($distance<$smallest)
			$smallest = $distance;
		}

		$result_array = array();
		foreach ($distance_table as $table => $distance) {
			if ($smallest == $distance)
			$result_array[] = $table;
		}

		return $result_array;
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
		// Once you have a valid DB object named $db...
		try {
			// TODO: try to use getTableDbModel instead of getTableInfo. This is more DB independent.
			$data = $this->getTableInfo($table_name);
		} catch (DBConnectionException $ex) {
			// If the table does not exist, let's return null.
			return null;
		}

		foreach ($data as $current_column) {
			if ($this->toStandardcaseColumn($current_column['column_name'])==$column_name)
			return true;
		}

		// If we are here, table was not found

		// Let's compute the lenvenstein distance and keep the smallest one in $smallest.
		$smallest = 99999999;
		$distance_column = array();

		foreach ($data as $current_column) {
			$distance = levenshtein($column_name, $current_column['column_name']);
			$distance_column[$current_column['column_name']]=$distance;
			if ($distance<$smallest)
			$smallest = $distance;
		}

		$result_array = array();
		foreach ($distance_column as $table => $distance) {
			if ($smallest == $distance)
			$result_array[] = $table;
		}

		return $result_array;
	}

	/**
	 * Returns, depending on the database system used and file system used the string passed
	 * in parameter in lowercase or in the same case.
	 * For instance, with a PgSQL database, you will always get a lowercase string.
	 * On MySQL, it will depend the system used. By default, on Windows, it should return a lowercase string
	 * while on Linux, it will return the same string.
	 *
	 * @see ConnectionInterface::toStandardcase()
	 * @param $string string String to put in lowercase if the database is case insensitive.
	 * @return bool
	 */
	function toStandardcase($string) {
		$caseSensitive = $this->isCaseSensitive();

		if ($caseSensitive) {
			return $string;
		} else {
			return strtolower($string);
		}
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
		return strtolower($string);
	}

	/**
	 * Begins a transaction. You must use commit or rollback to end the transaction.
	 * By default, if the scripts finishes and none of commit and rollback have been called,
	 * the transaction will be rolled-back.
	 * You can perform more than one beginTransaction() call. In this case, later calls will perform a "SAVEPOINT" and you will have to commit or rollback
	 * as many times as you called beginTransaction().
	 *
	 * @return bool true on success, false on failure.
	 */
	public function beginTransaction() {
		if ($this->dbh == null) {
			$this->connect();
		}
		if ($this->transactionLevel == 0) {
			$result = $this->dbh->beginTransaction();
			if ($result == true) {
				$this->transactionLevel++;
			}
		} else {
			$this->transactionLevel++;
			$this->dbh->exec("SAVEPOINT moufDbConnection".$this->transactionLevel);
			$result = true;
		}
		
		return $result;
	}

	/**
	 * Commits the transaction that has been started with beginTransaction.
	 *
	 * @return bool true on success, false on failure.
	 */
	public function commit() {
		if ($this->dbh == null) {
			$this->connect();
		}
		if ($this->transactionLevel == 1) {
			$this->dbh->commit();
			$this->transactionLevel--;
		} elseif ($this->transactionLevel > 1) {
			$this->dbh->exec("RELEASE SAVEPOINT moufDbConnection".$this->transactionLevel);
			$this->transactionLevel--;
		} else {
			throw new DBConnectionException("Unable to commit transaction: no transaction has been started.");
		}
	}

	/**
	 * Rolls-back the transaction that has been started with beginTransaction.
	 *
	 * @return bool true on success, false on failure.
	 */
	public function rollback() {
		if ($this->dbh == null) {
			$this->connect();
		}
		if ($this->transactionLevel == 1) {
				$this->dbh->rollBack();
				$this->transactionLevel--;
		} elseif ($this->transactionLevel > 1) {
			$this->dbh->exec("ROLLBACK TO moufDbConnection".$this->transactionLevel);
			$this->transactionLevel--;
		} else {
			throw new DBConnectionException("Unable to commit transaction: no transaction has been started.");
		}
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
		$table_info = $this->getTableInfo($table);
		foreach ($table_info as $columnArray) {
			if ($columnArray['column_name'] == $column) {
				if (isset($columnArray['data_type'])) {
					return $columnArray['data_type']; 
				} else {
					error_log("Invalid column? ".var_export($columnArray, true));
				}
			}
		}
		return null;
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
		switch ($type) {
			case "int2":
			case "int4":
			case "int8":
				if ($value != null && !is_numeric($value))
				return false;
				break;
			case "float4":
			case "float8":
				if ($value != null && !is_numeric($value))
				return false;
				break;

		}
		// TODO: MySQL Types, date types.
		return true;
	}

	/**
	 * Creates a new table in the database.
	 *
	 * @param Table $table The table to create
	 * @param boolean $dropIfExist whether the table should be dropped or not if it exists.
	 */
	//abstract public function createTable(Table $table, $dropIfExist);

	/**
	 * Creates a new index in the database.
	 *
	 * @param string $tableName
	 * @param array<string> $columnsList
	 * @param boolean $isUnique whether the index is unique or not.
	 * @param string $indexName The index name, generated if not specified.
	 */
	//abstract public function createIndex($tableName, $columnsList, $isUnique, $indexName=null);

	/**
	 * Returns the sequence name (code from Pear DB, thanks to the Pear DB team).
	 *
	 * @param unknown_type $sqn
	 * @return unknown
	 */
	public function getSequenceName($sqn)
    {
        //return sprintf($this->getOption('seqname_format'),
        //               preg_replace('/[^a-z0-9_.]/i', '_', $sqn));
        return sprintf("%s_pk_seq",
                       preg_replace('/[^a-z0-9_.]/i', '_', $sqn));
    }
    
    /**
     * Returns true of name passed in parameter matches the sequence name pattern.
     * 
     * @param $sqn
     * @return boolean
     */
    public function isSequenceName($sqn) {
    	if (strpos($sqn, "_pk_seq") === strlen($sqn)-7) {
    		return true;
    	} else {
    		return false;
    	}
    }
    
    /**
     * Creates a sequence with the name specified.
     * Note: The name is transformed be the getSequenceName method.
     * By default, if "mytable" is passed, the name of the sequence will be "mytable_pk_seq".
     *
     * @param string $seq_name
     */
    //abstract public function createSequence($seq_name);
    
    /**
	 * True if there is an active transaction (started with beginTransaction(), false otherwise).
	 * Note: this flag might be false in MySQL. If a DDL query is issued (like "DROP TABLE test"), the current transaction
	 * will be ended, but the flag will not be set to false).
	 *
	 * @return bool
	 */
	public function hasActiveTransaction() {
		return $this->transactionLevel != 0;
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
	 * Drops the database.
	 * Of course, a connection must be established for this call to succeed.
	 * Please note that you can create a connection without providing a dbname.
	 * Please also note that the function does not protect the parameter. You will have to protect
	 * it yourself against SQL injection attacks.
	 *
	 * @param string $dbName
	 */
	public function dropDatabase($dbName) {
		$this->exec("DROP DATABASE ".$dbName);
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
		$this->info("Processing $file statements...\n");
		$nb_errors = 0;
		$sql_string = file_get_contents($file);

		do {
			$next_statement = $this->clever_sql_split($sql_string);
				
			try {
				if (trim($next_statement)!="") {
					$this->trace("Executing statement: ".$next_statement);
					$this->exec($next_statement);
				}
			} catch (DBConnectionException $e) {
				$this->error("A database error occured when running the script '$file': ". $e->getMessage(), $e);
				$nb_errors++;
				if (!$on_error_continue)
				{
					break;
				}
			}
				
			$sql_string = substr($sql_string, strlen($next_statement)+1);
		} while ($sql_string != false);

		if (!$on_error_continue && $nb_errors!=0)
		{
			$this->error('Error while running SQL script "'.$file.'". Script aborted.');
			return false;
		} elseif ($on_error_continue && $nb_errors!=0) {
			$this->warn("SQL script completed. $nb_errors errors have been detected.");
			return false;
		} else {
			$this->info("SQL script completed.");
		}
	}

	/**
	 * Gets the next SQL command from $str (which is supposed to be an SQL file).
	 *
	 * @param unknown_type $str
	 * @return unknown
	 */
	private function clever_sql_split($str) {
		preg_match("/((?:(?:'(?:(?:\\\\')|[^'])*')|[^;])*)/",$str, $res);
		return $res[1];
	}

	/**
	 * Logs a message in the error log as a TRACE message.
	 * This function takes 1 or 2 arguments:
	 *
	 * @param string $string The string to log
	 * @param Exception $e The exception to log
	 */
	private function trace($string, Exception $e=null) {
		if ($this->log != null) {
			$this->log->trace($string, $e);
		}
	}

	/**
	 * Logs a message in the error log as a DEBUG message.
	 * This function takes 1 or 2 arguments:
	 *
	 * @param string $string The string to log
	 * @param Exception $e The exception to log
	 */
	private function debug($string, Exception $e=null) {
		if ($this->log != null) {
			$this->log->debug($string, $e);
		}
	}

	/**
	 * Logs a message in the error log as a INFO message.
	 * This function takes 1 or 2 arguments:
	 *
	 * @param string $string The string to log
	 * @param Exception $e The exception to log
	 */
	private function info($string, Exception $e=null) {
		if ($this->log != null) {
			$this->log->info($string, $e);
		}
	}

	/**
	 * Logs a message in the error log as a WARN message.
	 * This function takes 1 or 2 arguments:
	 *
	 * @param string $string The string to log
	 * @param Exception $e The exception to log
	 */
	private function warn($string, Exception $e=null) {
		if ($this->log != null) {
			$this->log->warn($string, $e);
		}
	}

	/**
	 * Logs a message in the error log as a ERROR message.
	 * This function takes 1 or 2 arguments:
	 *
	 * @param string $string The string to log
	 * @param Exception $e The exception to log
	 */
	private function error($string, Exception $e=null) {
		if ($this->log != null) {
			$this->log->error($string, $e);
		}
	}

	/**
	 * Logs a message in the error log as a FATAL message.
	 * This function takes 1 or 2 arguments:
	 *
	 * @param string $string The string to log
	 * @param Exception $e The exception to log
	 */
	private function fatal($string, Exception $e=null) {
		if ($this->log != null) {
			$this->log->fatal($string, $e);
		}
	}
}


?>
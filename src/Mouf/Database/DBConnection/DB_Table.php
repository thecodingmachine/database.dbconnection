<?php
namespace Mouf\Database\DBConnection; 

/**
 * This object represents a table in a database.
 *
 * @Component
 */
class DB_Table {	
	/**
	 * The name of the table.
	 *
	 * @Property
	 * @Compulsory
	 * @var string
	 */
	public $name;
	
	/**
	 * The columns of the table.
	 *
	 * @Property
	 * @Compulsory
	 * @var array<DB_Column>
	 */
	public $columns = array();
	
	/**
	 * Constructor.
	 *
	 * @param string $name The name of the table to create
	 * @param array<DB_Column> $columns The columns of the table
	 */
	public function __construct($name = null, $columns = array()) {
		$this->name = $name;
		$this->columns = $columns;
	}
	
	/**
	 * Adds a column to the table representation.
	 *
	 * @param DB_Column $column
	 */
	public function addColumn(DB_Column $column) {
		$this->columns[] = $column;
	}
	
	/**
	 * Returns an array of columns that are marked as primary keys.
	 *
	 * @return array<Db_Column>
	 */
	public function getPrimaryKeys() {
		$arr = array();
		foreach ($this->columns as $column) {
			if ($column->isPrimaryKey) {
				$arr[] = $column;
			}
		}
		return $arr;
	}
}
?>
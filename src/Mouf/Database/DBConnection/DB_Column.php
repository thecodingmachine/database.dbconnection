<?php
namespace Mouf\Database\DBConnection; 

/**
 * This object represents a column from a table in a database.
 *
 * @Component
 */
class DB_Column {
	
	/**
	 * The name of the column.
	 *
	 * @Property
	 * @Compulsory
	 * @var string
	 */
	public $name;
	
	/**
	 * The type of the column.
	 *
	 * @Property
	 * @Compulsory
	 * @var string
	 */
	public $type;
	
	/**
	 * Whether the column can be null or not.
	 *
	 * @Property
	 * @Compulsory
	 * @var boolean
	 */
	public $nullable;
	
	/**
	 * The default value for the column.
	 * Please put some quotes around if this is a string!
	 *
	 * @Property
	 * @Compulsory
	 * @var string
	 */
	public $default;
	
	/**
	 * Whether this column is part of the primary key.
	 *
	 * @Property
	 * @Compulsory
	 * @var boolean
	 */
	public $isPrimaryKey;
	
	/**
	 * Whether the column should be autoincremented or not.
	 *
	 * @Property
	 * @Compulsory
	 * @var boolean
	 */
	public $autoIncrement;

	/**
	 * The comment associated to the column.
	 *
	 * @Property
	 * @Compulsory
	 * @var string
	 */
	public $comment;
	
	/**
	 * Default constructor.
	 * 
	 * @param $name
	 * @param $type
	 * @param $nullable
	 * @param $default
	 * @param $isPrimaryKey
	 * @param $autoIncrement
	 * @return unknown_type
	 */
	public function __construct($name = null, $type = null, $nullable = null, $default = null, $isPrimaryKey = null, $autoIncrement = null, $comment = null) {
		$this->name = $name;
		$this->type = $type;
		$this->nullable = $nullable;
		$this->default = $default;
		$this->isPrimaryKey = $isPrimaryKey;
		$this->autoIncrement = $autoIncrement;
		$this->comment = $comment;
	}
}
?>
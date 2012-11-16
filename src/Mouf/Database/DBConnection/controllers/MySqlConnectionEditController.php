<?php

/**
 * The controller to edit MySql connections and to test them!
 * So cool!
 * 
 * @Component
 */
class MySqlConnectionEditController extends AbstractMoufInstanceController {
	
		
	/**
	 * Admin page used to enable or disable label edition.
	 *
	 * @Action
	 * //@Admin
	 */
	public function defaultAction($name, $selfedit="false") {
		$this->initController($name, $selfedit);
		
		$this->template->addContentFile(dirname(__FILE__)."/../views/mysqlEdit.php", $this);
		$this->template->draw();
	}
	
	/**
	 * Displays the list of all databases installed in JSON format.
	 * If the connection parameters are incorrect, returns an empty JSON array 
	 * 
	 * @Action
	 * @param string $host
	 * @param string $port
	 * @param string $user
	 * @param string $password
	 */
	public function getDbList($host, $port, $user, $password) {
		require_once dirname(__FILE__).'/../DB_Column.php';
		require_once dirname(__FILE__).'/../DB_Table.php';
		require_once dirname(__FILE__).'/../DB_ConnectionSettingsInterface.php';
		require_once dirname(__FILE__).'/../DB_ConnectionInterface.php';
		require_once dirname(__FILE__).'/../DB_Exception.php';
		require_once dirname(__FILE__).'/../Mouf_DBConnection.php';
		require_once dirname(__FILE__).'/../DB_MySqlConnection.php';
				
		
		
		$conn = new DB_MySqlConnection();
		$conn->host = $host;
		$conn->port = (!empty($port))?$port:null;
		$conn->user = $user;
		$conn->password = (!empty($password))?$password:null;
		
		try {
			$dbList = $conn->getDatabaseList();
		} catch (Exception $e) {
			// If bad parameters are passed, let's just return an empty list.
			echo "[]";
			return;
		}
		// Display the list.
		echo json_encode($dbList);
	}
	
	/**
	 * The action to save the instance.
	 * 
	 * @Action
	 * @param string $name Instance name
	 * @param bool $selfedit
	 * @param string $host
	 * @param string $port
	 * @param string $user
	 * @param string $password
	 * @param string $dbname
	 */
	public function save($name, $selfedit, $host, $port, $user, $password, $dbname) {
		$this->initController($name, $selfedit);
		
		$this->moufManager->setParameter($name, "host", $host);
		$this->moufManager->setParameter($name, "port", $port);
		$this->moufManager->setParameter($name, "user", $user);
		$this->moufManager->setParameter($name, "password", $password);
		$this->moufManager->setParameter($name, "dbname", $dbname);
		$this->moufManager->rewriteMouf();
		
		$this->defaultAction($name, $selfedit);
	}
}

?>
<?php
namespace Mouf\Database\DBConnection\Controllers;

use Mouf\Database\DBConnection\MySqlConnection;

use Mouf\Actions\InstallUtils;

use Mouf\MoufManager;

use Mouf\Mvc\Splash\Controllers\Controller;

/**
 * The controller managing the install process.
 * It will query the database details.
 *
 * @Component
 */
class DbConnectionInstallController extends Controller  {
	public $selfedit;
	
	/**
	 * The active MoufManager to be edited/viewed
	 *
	 * @var MoufManager
	 */
	public $moufManager;
	
	/**
	 * The template used by the main page for mouf.
	 *
	 * @Property
	 * @Compulsory
	 * @var TemplateInterface
	 */
	public $template;
	
	/**
	 * The content block the template will be writting into.
	 *
	 * @Property
	 * @Compulsory
	 * @var HtmlBlock
	 */
	public $contentBlock;
	
	/**
	 * Displays the first install screen.
	 * 
	 * @Action
	 * @Logged
	 * @param string $selfedit If true, the name of the component must be a component from the Mouf framework itself (internal use only) 
	 */
	public function defaultAction($selfedit = "false") {
		$this->selfedit = $selfedit;
		
		if ($selfedit == "true") {
			$this->moufManager = MoufManager::getMoufManager();
		} else {
			$this->moufManager = MoufManager::getMoufManagerHiddenInstance();
		}
				
		$this->contentBlock->addFile(dirname(__FILE__)."/../../../../views/installStep1.php", $this);
		$this->template->toHtml();
	}
	
	/**
	 * Skips the install process.
	 * 
	 * @Action
	 * @Logged
	 * @param string $selfedit If true, the name of the component must be a component from the Mouf framework itself (internal use only)
	 */
	public function skip($selfedit = "false") {
		InstallUtils::continueInstall($selfedit == "true");
	}

	protected $host;
	protected $port;
	protected $dbname;
	protected $user;
	protected $password;
	
	/**
	 * Displays the second install screen.
	 * 
	 * @Action
	 * @Logged
	 * @param string $selfedit If true, the name of the component must be a component from the Mouf framework itself (internal use only) 
	 */
	public function configure($selfedit = "false") {
		$this->selfedit = $selfedit;
		
		if ($selfedit == "true") {
			$this->moufManager = MoufManager::getMoufManager();
		} else {
			$this->moufManager = MoufManager::getMoufManagerHiddenInstance();
		}
		
		$this->host = "localhost";
		$this->port = "";
		$this->dbname = "";
		$this->user = "root";
		$this->password = "";
				
		$this->contentBlock->addFile(dirname(__FILE__)."/../../../../views/installStep2.php", $this);
		$this->template->toHtml();
	}
	
	
	
	/**
	 * Action to create the database connection.
	 * 
	 * @Action
	 * @Logged
	 * @param string $selfedit If true, the name of the component must be a component from the Mouf framework itself (internal use only)
	 */
	public function install($host, $port, $dbname, $user, $password, $selfedit = "false") {
		if ($selfedit == "true") {
			$this->moufManager = MoufManager::getMoufManager();
		} else {
			$this->moufManager = MoufManager::getMoufManagerHiddenInstance();
		}
		
		$moufManager = $this->moufManager;
		$configManager = $moufManager->getConfigManager();
		
		$constants = $configManager->getMergedConstants();
		
		if (!isset($constants['DB_HOST'])) {
			$configManager->registerConstant("DB_HOST", "string", "localhost", "The database host (the IP address or URL of the database server).");
		}
		
		if (!isset($constants['DB_PORT'])) {
			$configManager->registerConstant("DB_PORT", "int", "", "The database port (the port of the database server, keep empty to use default port).");
		}
		
		if (!isset($constants['DB_NAME'])) {
			$configManager->registerConstant("DB_NAME", "string", "", "The name of your database.");
		}
		
		if (!isset($constants['DB_USERNAME'])) {
			$configManager->registerConstant("DB_USERNAME", "string", "", "The username to access the database.");
		}
		
		if (!isset($constants['DB_PASSWORD'])) {
			$configManager->registerConstant("DB_PASSWORD", "string", "", "The password to access the database.");
		}
		
		if (!$moufManager->instanceExists("dbConnection")) {
			$moufManager->declareComponent("dbConnection", "Mouf\\Database\\DBConnection\\MySqlConnection");
			$moufManager->setParameter("dbConnection", "host", "DB_HOST", "config");
			$moufManager->setParameter("dbConnection", "port", "DB_PORT", "config");
			$moufManager->setParameter("dbConnection", "user", "DB_USERNAME", "config");
			$moufManager->setParameter("dbConnection", "password", "DB_PASSWORD", "config");
			$moufManager->setParameter("dbConnection", "dbname", "DB_NAME", "config");
		}
		
		$configPhpConstants = $configManager->getDefinedConstants();
		$configPhpConstants['DB_HOST'] = $host;
		$configPhpConstants['DB_PORT'] = $port;
		$configPhpConstants['DB_USERNAME'] = $user;
		$configPhpConstants['DB_PASSWORD'] = $password;
		$configPhpConstants['DB_NAME'] = $dbname;
		$configManager->setDefinedConstants($configPhpConstants);
		
		$moufManager->rewriteMouf();		
		
		InstallUtils::continueInstall($selfedit == "true");
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
		/*require_once dirname(__FILE__).'/../Column.php';
		require_once dirname(__FILE__).'/../Table.php';
		require_once dirname(__FILE__).'/../ConnectionSettingsInterface.php';
		require_once dirname(__FILE__).'/../ConnectionInterface.php';
		require_once dirname(__FILE__).'/../DBConnectionException.php';
		require_once dirname(__FILE__).'/../AbstractDBConnection.php';
		require_once dirname(__FILE__).'/../MySqlConnection.php';*/
				
		
		
		$conn = new MySqlConnection();
		$conn->host = $host;
		$conn->port = (!empty($port))?$port:null;
		$conn->user = $user;
		$conn->password = (!empty($password))?$password:null;
		
		try {
			$dbList = $conn->getDatabaseList();
		} catch (\Exception $e) {
			// If bad parameters are passed, let's just return an empty list.
			echo "[]";
			return;
		}
		// Display the list.
		echo json_encode($dbList);
	}
	
	
}

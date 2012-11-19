<?php
use Mouf\MoufManager;

// Controller declaration
MoufManager::getMoufManager()->declareComponent('mysqlconnectionedit', 'Mouf\\Database\\DBConnection\\Controllers\\MySqlConnectionEditController', true);
MoufManager::getMoufManager()->bindComponents('mysqlconnectionedit', 'template', 'moufTemplate');

MoufManager::getMoufManager()->declareComponent('dbconnectioninstall', 'Mouf\\Database\\DBConnection\\Controllers\\DbConnectionInstallController', true);
MoufManager::getMoufManager()->bindComponents('dbconnectioninstall', 'template', 'installTemplate');
?>
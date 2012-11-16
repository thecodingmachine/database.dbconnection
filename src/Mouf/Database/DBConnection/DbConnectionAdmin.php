<?php
// Controller declaration
MoufManager::getMoufManager()->declareComponent('mysqlconnectionedit', 'MySqlConnectionEditController', true);
MoufManager::getMoufManager()->bindComponents('mysqlconnectionedit', 'template', 'moufTemplate');

MoufManager::getMoufManager()->declareComponent('dbconnectioninstall', 'DbConnectionInstallController', true);
MoufManager::getMoufManager()->bindComponents('dbconnectioninstall', 'template', 'installTemplate');
?>
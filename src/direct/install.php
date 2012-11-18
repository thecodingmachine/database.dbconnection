<?php
require_once '../../../../../mouf/actions/InstallUtils.php';


InstallUtils::init(InstallUtils::$INIT_APP);

//echo 'youhou';
//InstallUtils::continueInstall();

// A good practice is to put the database parameters in the config.php file.
// Let's start by creating DB_HOST, DB_PORT, DB_NAME, DB_USERNAME and DB_PASSWORD

$moufManager = MoufManager::getMoufManager();
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



$moufManager->rewriteMouf();

InstallUtils::continueInstall();
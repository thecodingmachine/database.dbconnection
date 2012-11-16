<?php
/**
 * Classes implementing this interface will contain data required to connect to a database.
 * This interface implements methods to get the DSN, username and password required to get a connection to a database.
 * Use one of the classes implementing this interface to specify real settings.
 *
 */
interface DB_ConnectionSettingsInterface {
	
	/**
	 * Returns the DSN for this connection.
	 *
	 * @return string
	 */
	function getDsn();

	/**
	 * Returns the username for this connection (if any).
	 *
	 * @return string
	 */
	function getUserName();

	/**
	 * Returns the password for this connection (if any).
	 *
	 * @return string
	 */
	function getPassword();
	
	/**
	 * Returns an array of options to apply to the connection.
	 *
	 * @return array
	 */
	function getOptions();
}
?>
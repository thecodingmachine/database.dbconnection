<?php /* @var $this DbConnectionInstallController */ ?>
<script type="text/javascript" charset="utf-8">
function getDbList() {
    jQuery.getJSON("getDbList",{host: jQuery("#host").val(), port: jQuery("#port").val(), user: jQuery("#user").val(), password: jQuery("#password").val()}, function(j){
      var currentDb = '<?php echo  plainstring_to_htmlprotected($this->dbname) ?>';
      var options = '<option value=""></option>';
      for (var i = 0; i < j.length; i++) {
        options += '<option value="' + j[i] + '"' ;
        if (currentDb == j[i]) {
        	options += 'selected="true"';
        }
        options += '>' + j[i] + '</option>';
      }
      jQuery("#dbname").html(options);
    })
}

jQuery(function(){
  jQuery(".recomputeDbList").change(getDbList)
  getDbList();
})


</script>

<h1>Configure your MySQL connection</h1>

<form action="install">
<input type="hidden" id="selfedit" name="selfedit" value="<?php echo plainstring_to_htmlprotected($this->selfedit) ?>" />

<p>The IP address or URL of your database server. This is usually 'localhost'.</p>
<div>
	<label for="host">Host:</label>
	<input type="text" id="host" name="host" class="recomputeDbList" value="<?php echo plainstring_to_htmlprotected($this->host) ?>" />
</div>
<p>The port of the Mysql database server. Keep this empty to use default port.</p>
<div>
	<label for="port">Port:</label>
	<input type="text" id="port" name="port" class="recomputeDbList" value="<?php echo plainstring_to_htmlprotected($this->port) ?>" />
</div>
<p>The user to connect to the database.</p>
<div>
	<label for="user">User:</label>
	<input type="text" id="user" name="user" class="recomputeDbList" value="<?php echo plainstring_to_htmlprotected($this->user) ?>" />
</div>
<div>
	<label for="password">Password:</label>
	<input type="text" id="password" name="password" class="recomputeDbList" value="<?php echo plainstring_to_htmlprotected($this->password) ?>" />
</div>
<p>The database to connect to:</p>
<div>
	<label for="dbname">Database name:</label>
	<select id="dbname" name="dbname">
		
	</select>
</div>

<div>
	<button name="action" value="install" type="submit">Next</button>
</div>
<p>Note: Look at the "dbConnection" instance to access additional parameters (encoding, etc...).</p>
</form>
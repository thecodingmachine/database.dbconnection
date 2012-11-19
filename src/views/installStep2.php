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
    }).fail(function(msg) {
		addMessage(msg.responseText, "alert alert-error");
    });
}

jQuery(function(){
  jQuery(".recomputeDbList").change(getDbList)
  getDbList();
})


</script>

<h1>Configure your MySQL connection</h1>

<form action="install" class="form-horizontal">


<input type="hidden" id="selfedit" name="selfedit" value="<?php echo plainstring_to_htmlprotected($this->selfedit) ?>" />


<div class="control-group">
	<label for="host" class="control-label">Host:</label>
	<div class="controls">
		<input type="text" id="host" name="host" class="recomputeDbList" value="<?php echo plainstring_to_htmlprotected($this->host) ?>" />
		<span class="help-block">The IP address or URL of your database server. This is usually 'localhost'.</span>
	</div>
</div>

<div class="control-group">
	<label for="port" class="control-label">Port:</label>
	<div class="controls">
		<input type="text" id="port" name="port" class="recomputeDbList" value="<?php echo plainstring_to_htmlprotected($this->port) ?>" />
		<span class="help-block">The port of the Mysql database server. Keep this empty to use default port.</span>
	</div>
</div>

<div class="control-group">
	<label for="user" class="control-label">User:</label>
	<div class="controls">
		<input type="text" id="user" name="user" class="recomputeDbList" value="<?php echo plainstring_to_htmlprotected($this->user) ?>" />
		<span class="help-block">The user to connect to the database.</span>
	</div>
</div>
<div class="control-group">
	<label for="password" class="control-label">Password:</label>
	<div class="controls">
		<input type="text" id="password" name="password" class="recomputeDbList" value="<?php echo plainstring_to_htmlprotected($this->password) ?>" />
	</div>
</div>

<div class="control-group">
	<label for="dbname" class="control-label">Database name:</label>
	<div class="controls">
		<select id="dbname" name="dbname">
			
		</select>
		<span class="help-block">The database to connect to.</span>
	</div>
</div>

<div class="control-group">
	<div class="controls">
		<button name="action" value="install" type="submit" class="btn btn-danger">Next</button>
	</div>
</div>
<p>Note: After setup, you can customize additional parameters in the <strong>dbConnection</strong> instance (encoding, etc...).</p>
</form>
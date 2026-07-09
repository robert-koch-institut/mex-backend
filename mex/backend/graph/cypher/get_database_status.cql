<# Get the current status of the default database.

Returns:
    currentStatus: The current database status as a string
-#>
SHOW DEFAULT DATABASE
YIELD currentStatus;

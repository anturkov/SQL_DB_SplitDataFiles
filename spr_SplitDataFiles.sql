USE AdminDB
GO

ALTER PROCEDURE spr_SplitDataFiles
	@dbName NVARCHAR(256) = '',
	@fileGroup NVARCHAR(256) = 'PRIMARY',
	@tempFilename NVARCHAR(256) = '',
	@newFilename NVARCHAR(MAX)	 = ''
AS
BEGIN
	SET NOCOUNT ON;
	
	-- Var for Dynamic SQL
	DECLARE @cmd NVARCHAR(MAX)

	-- Var for Dynamic Param
	DECLARE @Param NVARCHAR(2000)

	-- Var for INT return Values
	DECLARE @result BIGINT = 0

	-- Var for Job Command
	DECLARE @jobCmd NVARCHAR(4000)

	-- Var for XP_CMDShell 
	DECLARE @XPcmd VARCHAR(8000)

	--Var for Messages
	DECLARE @msg NVARCHAR(MAX)

	-- Server Name
	DECLARE @serverName NVARCHAR(512) = CONVERT(NVARCHAR(512), SERVERPROPERTY('Servername'))

	-- Date String
	DECLARE @thisDate NVARCHAR(32) = FORMAT(GETDATE(), 'yyyyMMdd')

	-- Time String
	DECLARE @thisTime NVARCHAR(32) = FORMAT(GETDATE(), 'HHmmss')

	-- SQL Version
	DECLARE @sqlVersion INT = (SELECT CONVERT(INT, SERVERPROPERTY('ProductMajorVersion')))
	
	-- Advanced Options Value
	DECLARE @chkAdvOptions INT = (SELECT CONVERT(INT, value) FROM master.sys.configurations WHERE name = 'show advanced options')

	-- XP CMDSHELL Value
	DECLARE @chkXPCMDShell INT = (SELECT CONVERT(INT, value) FROM master.sys.configurations WHERE name = 'xp_cmdshell')

	-- Total data content size
	DECLARE @totalContentSizeMB BIGINT = 0

	-- Move To Temp Job Name
	DECLARE @jobNameMoveToTemp NVARCHAR(256) = 'SplitDataFiles_' + CONVERT(NVARCHAR(100), NEWID())

	-- Move to Temp Job Description
	DECLARE @jobDescMoveToTemp NVARCHAR(4000) = 'Database: ' + @dbname + CHAR(13) + CHAR(10) +
	'Description: This is only a temporary job - it will be deleted afterwards' + CHAR(13) + CHAR(10) +
	'Purpose: Move data from all data files in specified filegroup to a single temporary data file'

	-- Move to Temp Step Name
	DECLARE @jobStepMoveToTemp NVARCHAR(256) = @jobNameMoveToTemp

	-- Move data to all Files Job Name
	DECLARE @jobNameSpreadData NVARCHAR(256) = 'SplitDataFiles_' + CONVERT(nvarchar(100), NEWID())

	--Move data to all Files Job Description
	DECLARE @jobDescSpreadData NVARCHAR(4000) = 'Database: ' + @dbname + CHAR(13) + CHAR(10) +
	'Description: This is only a temporary job - it will be deleted afterwards' + CHAR(13) + CHAR(10) +
	'Purpose: Move data from a temporary data file in specified filegroup to "n" data files'

	--Move data to all Files Job Step
	DECLARE @jobStepSpreadData NVARCHAR(256) = @jobDescSpreadData

	-- Variable for all target files
	DECLARE @tblTargetFilenames TABLE (
		filepath NVARCHAR(256),
		logicalName NVARCHAR(256)
	)

	-- Variable for count of new files
	DECLARE @countTargetFilenames INT = 0

	-- Table for Disk free space
	DECLARE @diskFreeSpace TABLE (
		Drive NVARCHAR(MAX),
		FreeSpaceMB BIGINT
	)

	-- Tmp Table Disk FreeSpace for XP_CMDSHELL results
	DECLARE @tmpDiskSpace TABLE (
		result NVARCHAR(MAX)
	)

	-- Org Recovery Model
	DECLARE @orgRecoveryModel NVARCHAR(32)

	-- temp data file logical Name
	DECLARE @tempFileLogicalName NVARCHAR(256) = RTRIM(LTRIM(LEFT((RIGHT(@tempFilename, CHARINDEX('\', REVERSE(@tempFilename)) -1)), CHARINDEX('.', (RIGHT(@tempFilename, CHARINDEX('\', REVERSE(@tempFilename))))) -2)))

	-- Data Files to empty TABLE
	DECLARE @tblFilesToEmpty TABLE (
		fileId INT,
		logicalName NVARCHAR(256)
	)

	-- CHeck job Pct complete
	DECLARE @jobPctComplete NUMERIC(5,2) = 0.00

	-- new Initial data file Sizes
	DECLARE @newInitFileSizeMB BIGINT = 0

	-- new AutoGrow Size
	DECLARE @newAutoGrowSizeMB BIGINT = 1024

	--##################################################
	-- VALIDATION
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Validating requirements'
	RAISERROR(@msg, 10, 1) WITH NOWAIT;

	-- must not be a systemDB
	IF(@dbName IN ('master', 'model', 'msdb', 'tempdb'))
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Database must not be a System Database - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END

	--Check if user is SYSADMIN
	IF (IS_SRVROLEMEMBER('sysadmin') != 1)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | SYSADMIN privileges required - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END

	-- Check if database exists
	IF NOT EXISTS (
		SELECT 1
		FROM master.sys.databases
		WHERE name = @dbName
	)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Database not found - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END

	-- Check if DB is in AG
	IF EXISTS (
		SELECT 1
		FROM master.sys.dm_hadr_database_replica_states
		WHERE database_id = DB_ID(@dbName)
	)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Database is part of an Availability Group - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END

	-- Check if at least SQL Server 2016
	IF(@sqlVersion < 13)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Script is only supported on SQL Server 2016 or newer - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END

	--Check if Filegroup exists
	SET @cmd = '
	IF NOT EXISTS (
		SELECT 1
		FROM ' + @dbName + '.sys.filegroups
		WHERE name = ''' + @fileGroup + '''
		AND type = ''FG''
	)
	BEGIN
		RAISERROR(''no filegroup'', 16, 1) WITH NOWAIT
	END
	'
	
	BEGIN TRY
		EXEC sp_executesql @cmd
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Filegroup not found - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END CATCH
	
	-- Get Total Content Size to Move
	SET @cmd = '
	USE [' + @dbName + '];
	SELECT @dynResult = SUM(CAST(FILEPROPERTY(a.name, ''SpaceUsed'') AS INT) /128)
	FROM sys.database_files a
	LEFT JOIN sys.filegroups b
		ON a.data_space_id = b.data_space_id
	WHERE a.type = 0
	AND b.name = ''' + @fileGroup + ''';
	';
	SET @Param = '@dynResult BIGINT OUTPUT'
	EXEC sp_executesql @cmd, @param, @dynresult = @totalContentSizeMB OUTPUT

	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Total content size: ' + CONVERT(NVARCHAR(32), @totalContentSizeMB) + ' MB'
	RAISERROR(@msg, 10, 1) WITH NOWAIT;

	-- Split Data Files to table
	SET @newFilename = REPLACE(@newFilename, CHAR(9), '');
	SET @newFilename = REPLACE(@newFilename, CHAR(13), '');
	SET @newFilename = REPLACE(@newFilename, CHAR(10), '');
	INSERT INTO @tblTargetFilenames (filepath, logicalName)
	SELECT REPLACE((REPLACE((LTRIM(RTRIM(value))), CHAR(13)+CHAR(10), '')), CHAR(9), '') AS [Filepath],
		   RTRIM(LTRIM(LEFT((RIGHT(value, CHARINDEX('\', REVERSE(value)) -1)), CHARINDEX('.', (RIGHT(value, CHARINDEX('\', REVERSE(value))))) -2))) AS [LogicalName]
	FROM string_split(@newFilename, ';')
	WHERE value IS NOT NULL
	AND value != ''
	
	-- Count Files
	SET @countTargetFilenames = (SELECT COUNT(1) FROM @tblTargetFilenames)

	IF(@countTargetFilenames IN (0,1))
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | You must specify at least 2 new filenames - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT
		RETURN;
	END

	-- Check if there is enough disk space for temp file
	-- Enable XP CMDSHELL
	IF(@chkXPCMDShell = 0)
	BEGIN
		IF(@chkAdvOptions = 0)
		BEGIN
			EXEC sp_configure 'show advanced options', 1;
			RECONFIGURE;
		END
		EXEC sp_configure 'xp_cmdshell', 1;
		RECONFIGURE;
	END
	
	-- Check Free Space on Disk for temp File
	SET @XPcmd = '
		powershell.exe -c "Get-Volume -FilePath ''' + CONVERT(VARCHAR(MAX), @tempFilename) + ''' | Select -ExpandProperty SizeRemaining"
	';
	INSERT INTO @tmpDiskSpace (result)
	EXEC xp_cmdshell @XPcmd

	-- Add data to disk free space table
	INSERT INTO @diskFreeSpace (Drive, FreeSpaceMB)
	SELECT TOP (1) @tempFilename, CONVERT(BIGINT, result) / 1024 / 1024 FROM @tmpDiskSpace;
	
	IF EXISTS (
		SELECT 1 FROM @diskFreeSpace
	)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Total free space on disk for temp file: ' + (SELECT CONVERT(NVARCHAR(MAX), FreeSpaceMB) FROM @diskFreeSpace) + ' MB'
		RAISERROR(@msg, 10, 1) WITH NOWAIT
		IF((SELECT FreeSpaceMB FROM @diskFreeSpace) < @totalContentSizeMB )
		BEGIN
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Not enough disk space available for temp file - terminating script'
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			RETURN;
		END
	END
	ELSE
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Drive information for temporary file not found - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END
	
	-- Disable XP_Cmdshell
	IF(@chkXPCMDShell = 0)
	BEGIN
		EXEC sp_configure 'xp_cmdshell', 0;
		RECONFIGURE;

		IF(@chkAdvOptions = 0)
		BEGIN
			EXEC sp_configure 'show advanced options', 0;
			RECONFIGURE;
		END
	END


	-- Set recovery model to SIMPLE
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Setting SIMPLE recovery model'
	RAISERROR(@msg, 10, 1) WITH NOWAIT;
	SET @orgRecoveryModel = (
		SELECT recovery_model_desc
		FROM master.sys.databases
		WHERE name = @dbName
	);

	SET @cmd = '
		USE [master];
		ALTER DATABASE [' + @dbName + '] SET RECOVERY SIMPLE WITH NO_WAIT;
	';

	BEGIN TRY
		EXEC sp_executesql @cmd
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not set SIMPLE recovery model - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		GOTO CleanupSection;
	END CATCH

	-- Get all files to empty from filegroup
	SET @cmd = '
		USE [' + @dbName + '];
		SELECT a.file_id, a.name
		FROM sys.database_files a
		LEFT JOIN sys.filegroups b
			ON a.data_space_id = b.data_space_id
		WHERE b.name = ''' + @fileGroup + '''
		AND a.type = 0
	'
	INSERT INTO @tblFilesToEmpty (fileId, logicalName)
	EXEC sp_executesql @cmd

	IF NOT EXISTS (
		SELECT 1 FROM @tblFilesToEmpty
	)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | No data files found to empty'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		GOTO CleanupSection;
	END

	--######################################
	-- Create temp file
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Adding temp data file'
	RAISERROR(@msg, 10, 1) WITH NOWAIT;

	SET @cmd = '
		USE [master];
		ALTER DATABASE [' + @dbName + '] ADD FILE ( NAME = N''' + @tempFileLogicalName + ''', FILENAME = N''' + @tempFilename + ''' , SIZE = ' + CONVERT(NVARCHAR(MAX), @totalContentSizeMB) + 'MB , FILEGROWTH = 1024MB ) TO FILEGROUP [' + @fileGroup + '];
	'
	BEGIN TRY
		EXEC sp_executesql @cmd
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not add temp data file' + CHAR(13) + CHAR(10) +
		'Error number: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
		'Error Message: ' + ERROR_MESSAGE();
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		GOTO CleanupSection;
	END CATCH
	--####################################################
	-- Generate Temp Job Step Command
	DECLARE @tmpTempFileId INT
	DECLARE @tmpTempLogicalName NVARCHAR(256)
	DECLARE curTempDBFiles CURSOR FOR
		SELECT fileId, logicalName
		FROM @tblFilesToEmpty
		ORDER BY fileId DESC

	OPEN curTempDBFiles

	-- Loop through all data files
	FETCH NEXT FROM curTempDBFiles INTO @tmpTempFileId, @tmpTempLogicalName
	SET @jobCmd = ''
	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @jobCmd += 'USE [' + @dbName + '];' + CHAR(13) + CHAR(10) +
		'DBCC SHRINKFILE (N''' + @tmpTempLogicalName + ''' , EMPTYFILE);' + CHAR(13) + CHAR(10)

		IF(@tmpTempFileId != 1)
		BEGIN
			SET @jobCmd += 'USE [' + @dbName + '];' + CHAR(13) + CHAR(10) +
			'ALTER DATABASE [' + @dbName + ']  REMOVE FILE [' + @tmpTempLogicalName + '];' + CHAR(13) + CHAR(10)
		END
		
		FETCH NEXT FROM curTempDBFiles INTO @tmpTempFileId, @tmpTempLogicalName
	END

	CLOSE curTempDBFiles
	DEALLOCATE curTempDBFiles
	

	--##################################
	-- Create job for move to temp data file
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Create SQL Agent Job to move data to temp data file: ' + @jobNameMoveToTemp
	RAISERROR(@msg, 10, 1) WITH NOWAIT;
	BEGIN TRY
	EXEC    msdb.dbo.sp_add_job @job_name=@jobNameMoveToTemp, 
			@enabled=1, 
			@notify_level_eventlog=0, 
			@notify_level_email=0, 
			@notify_level_netsend=0, 
			@notify_level_page=0, 
			@delete_level=0, 
			@description=@jobDescMoveToTemp, 
			@category_name=N'[Uncategorized (Local)]', 
			@owner_login_name=N'sa';

	EXEC msdb.dbo.sp_add_jobstep @job_name = @jobNameMoveToTemp, @step_name=@jobNameMoveToTemp, 
			@step_id=1, 
			@cmdexec_success_code=0, 
			@on_success_action=1, 
			@on_success_step_id=0, 
			@on_fail_action=2, 
			@on_fail_step_id=0, 
			@retry_attempts=0, 
			@retry_interval=0, 
			@os_run_priority=0, @subsystem=N'TSQL', 
			@command=@jobCmd, 
			@database_name=N'master', 
			@flags=0;

	EXEC msdb.dbo.sp_add_jobserver @job_name = @jobNameMoveToTemp, @server_name =  @serverName;
	
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not create SQL Agent Job to move data to temp data file: ' + @jobNameMoveToTemp + CHAR(13) + CHAR(10) +
		'Error Number: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
		'Error Message: ' + ERROR_MESSAGE()
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		GOTO CleanupSection;
	END CATCH

	--###################################
	-- Start job move to temp
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Starting job ' + @jobNameMoveToTemp
	RAISERROR(@msg, 10, 1) WITH NOWAIT;

	BEGIN TRY
		EXEC msdb.dbo.sp_start_job @job_name = @jobNameMoveToTemp
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not start SQL Agent Job to move data to temp data file: ' + @jobNameMoveToTemp + CHAR(13) + CHAR(10) +
		'Error Number: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
		'Error Message: ' + ERROR_MESSAGE()
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		GOTO CleanupSection;
	END CATCH

	WAITFOR DELAY '00:00:10';

	WHILE (
		(SELECT a.stop_execution_date
		 FROM msdb.dbo.sysjobactivity a
		 INNER JOIN msdb.dbo.sysjobs b
			 ON a.job_id = b.job_id
		 WHERE b.name = @jobNameMoveToTemp) IS NULL
	)
	BEGIN
		SET @cmd = '
		USE [' + @dbName + '];
		SELECT @dynResult = SUM(CAST(FILEPROPERTY(a.name, ''SpaceUsed'') AS INT) /128)
		FROM sys.database_files a
		LEFT JOIN sys.filegroups b
			ON a.data_space_id = b.data_space_id
		WHERE a.type = 0
		AND b.name = ''' + @fileGroup + '''
		AND a.name = ''' + @tempFileLogicalName + ''' ;
		';
		SET @Param = '@dynResult BIGINT OUTPUT'
		EXEC sp_executesql @cmd, @param, @dynresult = @result OUTPUT

		IF(@totalContentSizeMB > 0)
		BEGIN
			SET @jobPctComplete = @result * 100.00 / @totalContentSizeMB
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Processed ' + CONVERT(NVARCHAR(MAX), @result) + ' MB (' + CONVERT(NVARCHAR(MAX), @jobPctComplete) + ' pct)';
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			WAITFOR DELAY '00:01:00';
		END
	END

	-- CHECK status of job
	IF EXISTS (
		SELECT 1
		FROM msdb.dbo.sysjobhistory
		WHERE step_name = @jobStepMoveToTemp
		AND (message LIKE '%Cannot move all contents of file "%" to other places to complete the emptyfile operation%'
			 OR message LIKE '%The step succeeded%')
	)
	BEGIN
		-- Count files in Filegroup
		SET @cmd = '
			USE [' + @dbName + '];
			SELECT @dynResult = COUNT(1)
			FROM sys.database_files a
			LEFT JOIN sys.filegroups b
				ON a.data_space_id = b.data_space_id
			WHERE a.type = 0
			AND b.name = ''' + @fileGroup + ''';
		';
		SET @Param = '@dynResult BIGINT OUTPUT'
		EXEC sp_executesql @cmd, @param, @dynresult = @result OUTPUT
		
		IF(
			((@fileGroup = 'PRIMARY') AND (@result = 2))
			OR
			((@fileGroup != 'PRIMARY') AND (@result = 1))
		)
		BEGIN
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Data has been moved to temporary file successfully';
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
		END
		ELSE
		BEGIN
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Data movement to temporary data file failed';
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			GOTO CleanupSection;
		END
	END

	-- Delete Job Temp Movement
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Deleting SQL Agent job: ' + @jobNameMoveToTemp;
	RAISERROR(@msg, 10, 1) WITH NOWAIT;
	BEGIN TRY
		EXEC msdb.dbo.sp_delete_job @job_name = @jobNameMoveToTemp, @delete_history = 0
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | WARN | Could not delete SQL Agent job: ' + @jobNameMoveToTemp + CHAR(13) + CHAR(10) +
		'Error Number: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
		'Error Message: ' + ERROR_MESSAGE();
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END CATCH

	--########################################
	-- Plan the new files
	IF(@fileGroup = 'PRIMARY')
	BEGIN
		SET @newInitFileSizeMB = FLOOR(POWER(2, CEILING(LOG(@totalContentSizeMB / (@countTargetFilenames + 1),2))))
	END
	ELSE
	BEGIN 
		SET @newInitFileSizeMB = FLOOR(POWER(2, CEILING(LOG(@totalContentSizeMB / @countTargetFilenames,2))))
	END
	
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Creating ' + CONVERT(NVARCHAR(10), @countTargetFilenames) + ' new files. All data files in this filegroup will have an initial size of ' + CONVERT(NVARCHAR(MAX), @newInitFileSizeMB) + ' MB.';
	RAISERROR(@msg, 10, 1) WITH NOWAIT;

	SET @cmd = 'USE [master];' + CHAR(13) + CHAR(10)
	SELECT @cmd += ISNULL(('ALTER DATABASE [' + @dbName + '] ADD FILE (NAME = N''' + logicalName + ''', FILENAME = N''' + filepath + ''', SIZE = ' + CONVERT(NVARCHAR(MAX), @newInitFileSizeMB) + 'MB, FILEGROWTH = ' + CONVERT(NVARCHAR(MAX), @newAutoGrowSizeMB) + 'MB ) TO FILEGROUP [' + @fileGroup + '];' + CHAR(13) + CHAR(10)), '')
	FROM @tblTargetFilenames
	ORDER BY logicalName ASC
	
	BEGIN TRY
		EXEC sp_executesql @cmd
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not create new data files' + CHAR(13) + CHAR(10) +
			'Error Number: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
			'Error Message: ' + ERROR_MESSAGE();
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		GOTO CleanupSection;
	END CATCH

	-- If it is in primary - shrink file to newInitialFileSize
	IF(@fileGroup = 'PRIMARY')
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Changing file size of MDF file'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		-- CHeck if MDF file is larger than newInitialSize
		IF(
			(SELECT size * 8 / 1024 
			 FROM master.sys.master_files
			 WHERE database_id = DB_ID(@dbName)
			 AND file_id = 1
			 AND type = 0
			) >= @newInitFileSizeMB
		)
		BEGIN
			-- Shrink file to initial size
			SET @cmd = '
				USE [LargeDB1FG];
				DBCC SHRINKFILE (N''' + (SELECT CONVERT(NVARCHAR(MAX), name) FROM master.sys.master_files WHERE type = 0 AND file_id = 1 AND database_id = DB_ID(@dbName)) + ''' , ' + CONVERT(NVARCHAR(MAX), @newInitFileSizeMB) + ');
			';
		END
		ELSE
		BEGIN
			SET @cmd = '
				USE [master];
				ALTER DATABASE [' + @dbName + '] MODIFY FILE ( NAME = N''' + (SELECT CONVERT(NVARCHAR(MAX), name) FROM master.sys.master_files WHERE type = 0 AND file_id = 1 AND database_id = DB_ID(@dbName)) + ''', SIZE = ' + CONVERT(NVARCHAR(MAX), @newInitFileSizeMB) + 'MB );
			';
		END

		-- Adjust AutoGrow 
		SET @cmd += '
			USE [master];
			ALTER DATABASE [' + @dbName + '] MODIFY FILE ( NAME = N''' + (SELECT CONVERT(NVARCHAR(MAX), name) FROM master.sys.master_files WHERE type = 0 AND file_id = 1 AND database_id = DB_ID(@dbName)) + ''', FILEGROWTH = ' + CONVERT(NVARCHAR(MAX), @newAutoGrowSizeMB) + 'MB );
		';

		BEGIN TRY
			EXEC sp_executesql @cmd
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not adjust MDF data file size' + CHAR(13) + CHAR(10) +
			'Error Number: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
			'Error Message: ' + ERROR_MESSAGE();
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			GOTO CleanupSection;
		END CATCH
	END

	-- Enable AUTOGROW_ALL_FILES
	-- Check if already set
	SET @cmd = '
		USE [' + @dbName + '];
		SELECT @dynResult = CONVERT(INT, is_autogrow_all_files)
		FROM sys.filegroups
		WHERE name = N''' + @fileGroup + '''
	'
	SET @Param = '@dynResult BIGINT OUTPUT'
	EXEC sp_executesql @cmd, @param, @dynresult = @result OUTPUT

	IF(@result = 0)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Enabling "AUTOGROW_ALL_FILES" feature on filegroup (DB in single-user mode)';
		RAISERROR(@msg, 10, 1) WITH NOWAIT;

		SET @cmd = '
			USE [master];
			ALTER DATABASE [' + @dbName + '] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
			USE [' + @dbName + '];
			ALTER DATABASE [' + @dbName + '] MODIFY FILEGROUP [' + @fileGroup + '] AUTOGROW_ALL_FILES;
			USE [master];
			ALTER DATABASE [' + @dbName + '] SET MULTI_USER;
		'

		BEGIN TRY
			EXEC sp_executesql @cmd
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not set "AUTOGROW_ALL_FILES" feature' + CHAR(13) + CHAR(10) +
				'Error Number: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
				'Error Message: ' + ERROR_MESSAGE();
				RAISERROR(@msg, 10, 1) WITH NOWAIT;
				GOTO CleanupSection;
		END CATCH
	END
	
	--##################################
	-- Create Job to empty temp File
	SET @jobCmd = 'USE [' + @dbName + '];' + CHAR(13) + CHAR(10) +
	'DBCC SHRINKFILE (N''' + @tempFileLogicalName + ''' , EMPTYFILE);' + CHAR(13) + CHAR(10)
	
	SET @jobCmd += 'USE [' + @dbName + '];' + CHAR(13) + CHAR(10) +
	'ALTER DATABASE [' + @dbName + ']  REMOVE FILE [' + @tempFileLogicalName + '];' + CHAR(13) + CHAR(10)

	-- Create job to move data from temp file to new data files
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Create SQL Agent Job to move data from temp data file to new data files: ' + @jobNameSpreadData
	RAISERROR(@msg, 10, 1) WITH NOWAIT;
	BEGIN TRY
	EXEC    msdb.dbo.sp_add_job @job_name=@jobNameSpreadData, 
			@enabled=1, 
			@notify_level_eventlog=0, 
			@notify_level_email=0, 
			@notify_level_netsend=0, 
			@notify_level_page=0, 
			@delete_level=0, 
			@description=@jobDescSpreadData, 
			@category_name=N'[Uncategorized (Local)]', 
			@owner_login_name=N'sa';

	EXEC msdb.dbo.sp_add_jobstep @job_name = @jobNameSpreadData, @step_name=@jobNameSpreadData, 
			@step_id=1, 
			@cmdexec_success_code=0, 
			@on_success_action=1, 
			@on_success_step_id=0, 
			@on_fail_action=2, 
			@on_fail_step_id=0, 
			@retry_attempts=0, 
			@retry_interval=0, 
			@os_run_priority=0, @subsystem=N'TSQL', 
			@command=@jobCmd, 
			@database_name=N'master', 
			@flags=0;

	EXEC msdb.dbo.sp_add_jobserver @job_name = @jobNameSpreadData, @server_name =  @serverName;
	
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not create SQL Agent Job to move data from temp data file to new data files: ' + @jobNameSpreadData + CHAR(13) + CHAR(10) +
		'Error Number: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
		'Error Message: ' + ERROR_MESSAGE()
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		GOTO CleanupSection;
	END CATCH


	-- Start job
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Starting job: ' + @jobNameSpreadData
	RAISERROR(@msg, 10, 1) WITH NOWAIT;

	BEGIN TRY
		EXEC msdb.dbo.sp_start_job @job_name = @jobNameSpreadData
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not start SQL Agent Job: ' + @jobNameSpreadData + CHAR(13) + CHAR(10) +
		'Error Number: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
		'Error Message: ' + ERROR_MESSAGE()
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		GOTO CleanupSection;
	END CATCH

	-- Monitor progress
	WAITFOR DELAY '00:00:10';

	WHILE (
		(SELECT a.stop_execution_date
		 FROM msdb.dbo.sysjobactivity a
		 INNER JOIN msdb.dbo.sysjobs b
			 ON a.job_id = b.job_id
		 WHERE b.name = @jobNameSpreadData) IS NULL
	)
	BEGIN
		SET @cmd = '
		USE [' + @dbName + '];
		SELECT @dynResult = SUM(CAST(FILEPROPERTY(a.name, ''SpaceUsed'') AS INT) /128)
		FROM sys.database_files a
		LEFT JOIN sys.filegroups b
			ON a.data_space_id = b.data_space_id
		WHERE a.type = 0
		AND b.name = ''' + @fileGroup + '''
		AND a.name != ''' + @tempFileLogicalName + ''' ;
		';
		SET @Param = '@dynResult BIGINT OUTPUT'
		EXEC sp_executesql @cmd, @param, @dynresult = @result OUTPUT

		IF(@totalContentSizeMB > 0)
		BEGIN
			SET @jobPctComplete = @result * 100.00 / @totalContentSizeMB
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Processed ' + CONVERT(NVARCHAR(MAX), @result) + ' MB (' + CONVERT(NVARCHAR(MAX), @jobPctComplete) + ' pct)';
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			WAITFOR DELAY '00:01:00';
		END
	END


	-- Check job status
	SELECT 1
		FROM msdb.dbo.sysjobhistory
		WHERE step_name = @jobNameSpreadData
		AND [message] LIKE '%The step succeeded%'
	-- CHECK status of job
	IF EXISTS (
		SELECT 1
		FROM msdb.dbo.sysjobhistory
		WHERE step_name = @jobNameSpreadData
		AND [message] LIKE '%The step succeeded%'
	)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Splitting data to multiple data files successful';
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END
	ELSE
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Splitting data to multiple data failed' + CHAR(13) + CHAR(10) +
		(SELECT CONVERT(NVARCHAR(MAX), message)
		 FROM msdb.dbo.sysjobhistory
		 WHERE step_name = @jobStepSpreadData)
		 RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END

	-- Delete Job Spread Data
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Deleting SQL Agent job: ' + @jobNameSpreadData
	RAISERROR(@msg, 10, 1) WITH NOWAIT;

	BEGIN TRY
		EXEC msdb.dbo.sp_delete_job @job_name = @jobNameSpreadData, @delete_history = 0
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not delete SQL Agent job: ' + @jobNameSpreadData
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END CATCH

	--########################################
	-- Cleanup
	GOTO CleanupSection;
	CleanupSection:
	-- Change Recovery Model to org Value
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Setting ' + @orgRecoveryModel + ' recovery model'
	SET @cmd = '
		USE [master];
		ALTER DATABASE [' + @dbName + '] SET RECOVERY ' + @orgRecoveryModel + ' WITH NO_WAIT;
	';
	BEGIN TRY
		EXEC sp_executesql @cmd
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not set recovery model to ' + @orgRecoveryModel
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END CATCH
END

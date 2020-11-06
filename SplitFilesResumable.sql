/*

	PLEASE SET VALUES FOR FOLLOWING VARIABLES

*/

-- database name
DECLARE @dbName NVARCHAR(256) = 'testdb2'

-- Filegroup name
DECLARE @filegroup NVARCHAR(256) = 'PRIMARY'

-- Filename for temp file
DECLARE @tempFilename NVARCHAR(256) = 'D:\MSSQL\SQL19EE\Data\testdb2_temp.ndf'

-- Filenames for the new NDF files (separate with ;)
DECLARE @newFilename NVARCHAR(MAX) = 'D:\MSSQL\SQL19EE\Data\testdb2_02.ndf;
									  D:\MSSQL\SQL19EE\Data\testdb2_03.ndf;
									  D:\MSSQL\SQL19EE\Data\testdb2_04.ndf;
									  D:\MSSQL\SQL19EE\Data\testdb2_05.ndf;
									  D:\MSSQL\SQL19EE\Data\testdb2_06.ndf;'

-- Owner of the jobs
DECLARE @jobOwner NVARCHAR(256) = 'sa'

--##########################################################################################
SET NOCOUNT ON;

-- Var for Dynamic SQL
DECLARE @cmd NVARCHAR(MAX)

-- Var for Dynamic Param
DECLARE @Param NVARCHAR(2000)

-- Var for INT return Values
DECLARE @result BIGINT = 0

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

-- Job Names
-- Job Name Prefix
DECLARE @jobNamePrefix NVARCHAR(256) = '_SplitDataFiles_' + @dbName + '_'

-- Job Name: 01_SplitDataFiles_DBNAME_Prerequisites
DECLARE @jobName01 NVARCHAR(256) = '01' + @jobNamePrefix + 'CreateTempFile'

-- Job Name: 02_SplitDataFiles_DBName_MoveToTempFile
DECLARE @jobName02 NVARCHAR(256) = '02' + @jobNamePrefix + 'MoveToTempFile'

-- Job Name: 03_SplitDataFiles_DBNAME_CreateNewFiles
DECLARE @jobName03 NVARCHAR(256) = '03' + @jobNamePrefix + 'CreateNewFiles'

-- Job Name: 04_SplitDataFiles_DBNAME_MoveToDataFiles
DECLARE @jobName04 NVARCHAR(256) = '04' + @jobNamePrefix + 'MoveToDataFiles'


-- Job Descriptions
-- Job 01:
DECLARE @job01Desc NVARCHAR(4000) = 'Database: ' + @dbName + CHAR(13) + CHAR(10) +
									'Purpose: Create temporary data file'

-- Job 02:
DECLARE @job02Desc NVARCHAR(4000) = 'Database: ' + @dbName + CHAR(13) + CHAR(10) +
									'Purpose: Perform DBCC SHRINKFILE (emptyfile) on all files in this filegroup and thus move the data to the temporary data file' + CHAR(13) + CHAR(10) +
									'All data files will be deleted afterwards (except MDF file)'

-- Job 03:
DECLARE @job03Desc NVARCHAR(4000) = 'Database: ' + @dbName + CHAR(13) + CHAR(10) +
									'Purpose: Create new data files'

-- Job 04:
DECLARE @job04Desc NVARCHAR(4000) = 'Database: ' + @dbName + CHAR(13) + CHAR(10) +
									'Purpose: Perform DBCC SHRINKFILE (emptyfile) on temporary file and delete it after everything has been moved'

-- Job Step
DECLARE @jobStepName NVARCHAR(256) = 'SplitDataFiles Step'

-- Job Command 01:
DECLARE @jobCmd01 NVARCHAR(4000) = ''

-- Job Command 02:
DECLARE @jobCmd02 NVARCHAR(4000) = ''

-- Job Command 03:
DECLARE @jobCmd03 NVARCHAR(4000) = ''

-- Job Command 04:
DECLARE @jobCmd04 NVARCHAR(4000) = ''

-- Job Template
DECLARE @createJobTemplate NVARCHAR(MAX)
SET @createJobTemplate = '
EXEC    msdb.dbo.sp_add_job @job_name=N''#JOBNAME#'', 
			@enabled=1, 
			@notify_level_eventlog=0, 
			@notify_level_email=0, 
			@notify_level_netsend=0, 
			@notify_level_page=0, 
			@delete_level=0, 
			@description=N''#JOBDESC#'', 
			@category_name=N''[Uncategorized (Local)]'', 
			@owner_login_name=N''#JOBOWNER#'';

	EXEC msdb.dbo.sp_add_jobstep @job_name = N''#JOBNAME#'', @step_name=N''#STEPNAME#'', 
			@step_id=1, 
			@cmdexec_success_code=0, 
			@on_success_action=1, 
			@on_success_step_id=0, 
			@on_fail_action=2, 
			@on_fail_step_id=0, 
			@retry_attempts=0, 
			@retry_interval=0, 
			@os_run_priority=0, @subsystem=N''TSQL'', 
			@command=N''#JOBCMD#'', 
			@database_name=N''master'', 
			@flags=0;

	EXEC msdb.dbo.sp_add_jobserver @job_name = N''#JOBNAME#'', @server_name =  N''#JOBSERVER#'';
'

-- Variable for all target files
DECLARE @tblTargetFilenames TABLE (
	filepath NVARCHAR(256),
	logicalName NVARCHAR(256)
)

-- Variable for count of new files
DECLARE @countTargetFilenames INT = 0

-- temp data file logical Name
DECLARE @tempFileLogicalName NVARCHAR(256) = RTRIM(LTRIM(LEFT((RIGHT(@tempFilename, CHARINDEX('\', REVERSE(@tempFilename)) -1)), CHARINDEX('.', (RIGHT(@tempFilename, CHARINDEX('\', REVERSE(@tempFilename))))) -2)))

-- Data Files to empty TABLE
DECLARE @tblFilesToEmpty TABLE (
	fileId INT,
	logicalName NVARCHAR(256)
)

-- new Initial data file Sizes
DECLARE @newInitFileSizeMB BIGINT = 0

-- new AutoGrow Size
DECLARE @newAutoGrowSizeMB BIGINT = 1024

--################################################################################################
-- VALIDATION
SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Validating requirements'
RAISERROR(@msg, 10, 1) WITH NOWAIT;

--Check if user is SYSADMIN
IF (IS_SRVROLEMEMBER('sysadmin') != 1)
BEGIN
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | SYSADMIN privileges required - terminating script'
	RAISERROR(@msg, 10, 1) WITH NOWAIT;
	RETURN;
END

-- must not be a systemDB
IF(@dbName IN ('master', 'model', 'msdb', 'tempdb'))
BEGIN
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Database must not be a System Database - terminating script'
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
	RETURN;
END

-- Plan the new files
IF(@fileGroup = 'PRIMARY')
BEGIN
	SET @newInitFileSizeMB = FLOOR(POWER(2, CEILING(LOG(@totalContentSizeMB / (@countTargetFilenames + 1),2))))
END
ELSE
BEGIN 
	SET @newInitFileSizeMB = FLOOR(POWER(2, CEILING(LOG(@totalContentSizeMB / @countTargetFilenames,2))))
END

SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Jobs will create ' + CONVERT(NVARCHAR(10), @countTargetFilenames) + ' new files. All data files in this filegroup will have an initial size of ' + CONVERT(NVARCHAR(MAX), @newInitFileSizeMB) + ' MB.';
RAISERROR(@msg, 10, 1) WITH NOWAIT;

--###################################################################
-- DROP ALL JOBS IF EXIST
BEGIN TRY
	EXEC msdb.dbo.sp_delete_job @job_name = @jobName01, @delete_history = 0
END TRY
BEGIN CATCH
END CATCH
BEGIN TRY
	EXEC msdb.dbo.sp_delete_job @job_name = @jobName02, @delete_history = 0
END TRY
BEGIN CATCH
END CATCH
BEGIN TRY
	EXEC msdb.dbo.sp_delete_job @job_name = @jobName03, @delete_history = 0
END TRY
BEGIN CATCH
END CATCH
BEGIN TRY
	EXEC msdb.dbo.sp_delete_job @job_name = @jobName04, @delete_history = 0
END TRY
BEGIN CATCH
END CATCH

--##################################################################
-- GENERATE JOB 01
SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Generating job: ' + @jobName01
RAISERROR(@msg, 10, 1) WITH NOWAIT;

-- Job Cmd
SET @jobCmd01 = 'USE [master];' + CHAR(13) + CHAR(10) + 
				'ALTER DATABASE [' + @dbName + '] ADD FILE ( NAME = N''''' + @tempFileLogicalName + ''''', FILENAME = N''''' + @tempFilename + ''''' , SIZE = ' + CONVERT(NVARCHAR(MAX), @totalContentSizeMB) + 'MB , FILEGROWTH = 1024MB ) TO FILEGROUP [' + @fileGroup + '];'

SET @cmd = @createJobTemplate
SET @cmd = REPLACE(@cmd, '#JOBNAME#', @jobName01)
SET @cmd = REPLACE(@cmd, '#JOBDESC#', @job01Desc)
SET @cmd = REPLACE(@cmd, '#JOBOWNER#', @jobOwner)
SET @cmd = REPLACE(@cmd, '#STEPNAME#', @jobStepName)
SET @cmd = REPLACE(@cmd, '#JOBCMD#', @jobCmd01)
SET @cmd = REPLACE(@cmd, '#JOBSERVER#', @serverName)

BEGIN TRY
	EXECUTE sp_executesql @cmd
END TRY
BEGIN CATCH
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not create SQL Agent Job: ' + @jobName01 + CHAR(13) + CHAR(10) +
	'Error Number: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
	'Error Message: ' + ERROR_MESSAGE()
	RAISERROR(@msg, 10, 1) WITH NOWAIT;
	RETURN;
END CATCH

--########################################################################################
-- Generate Job 2
SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Generating job: ' + @jobName02
RAISERROR(@msg, 10, 1) WITH NOWAIT;

-- Job CMd

-- Check if DB is in Simple Mode
SET @jobCmd02 = 'IF(' + CHAR(13) + CHAR(10) +
				'(SELECT recovery_model_desc' + CHAR(13) + CHAR(10) +
				'FROM master.sys.databases' + CHAR(13) + CHAR(10) +
				'WHERE name = ''''' + @dbName + ''''') != ''''SIMPLE'''')' + CHAR(13) + CHAR(10) +
				'BEGIN' + CHAR(13) + CHAR(10) +
				'RAISERROR(''''Database needs to be in SIMPLE recovery model'''', 16, 4) WITH NOWAIT;' + CHAR(13) + CHAR(10) +
				'RETURN;'  + CHAR(13) + CHAR(10) +
				'END' + CHAR(13) + CHAR(10)

DECLARE @tmpTempFileId INT
DECLARE @tmpTempLogicalName NVARCHAR(256)
DECLARE curTempDBFiles CURSOR FOR
	SELECT fileId, logicalName
	FROM @tblFilesToEmpty
	ORDER BY fileId DESC

OPEN curTempDBFiles

-- Loop through all data files
FETCH NEXT FROM curTempDBFiles INTO @tmpTempFileId, @tmpTempLogicalName
WHILE @@FETCH_STATUS = 0
BEGIN

	SET @jobCmd02 += 'USE [' + @dbName + '];' + CHAR(13) + CHAR(10) +
	'BEGIN TRY' + CHAR(13) + CHAR(10) +
	'DBCC SHRINKFILE (N''''' + @tmpTempLogicalName + ''''' , EMPTYFILE);' + CHAR(13) + CHAR(10) +
	'END TRY' + CHAR(13) + CHAR(10) +
	'BEGIN CATCH' + CHAR(13) + CHAR(10) +
	'IF(ERROR_MESSAGE() NOT LIKE ''''%Cannot move all contents of file "%" to other places to complete the emptyfile operation%'''')' + CHAR(13) + CHAR(10) +
	'THROW;' + CHAR(13) + CHAR(10) +
	'END CATCH' + CHAR(13) + CHAR(10)

	IF(@tmpTempFileId != 1)
	BEGIN
		SET @jobCmd02 += 'USE [' + @dbName + '];' + CHAR(13) + CHAR(10) +
		'ALTER DATABASE [' + @dbName + ']  REMOVE FILE [' + @tmpTempLogicalName + '];' + CHAR(13) + CHAR(10)
	END

	FETCH NEXT FROM curTempDBFiles INTO @tmpTempFileId, @tmpTempLogicalName
END
CLOSE curTempDBFiles
DEALLOCATE curTempDBFiles

SET @cmd = @createJobTemplate
SET @cmd = REPLACE(@cmd, '#JOBNAME#', @jobName02)
SET @cmd = REPLACE(@cmd, '#JOBDESC#', @job02Desc)
SET @cmd = REPLACE(@cmd, '#JOBOWNER#', @jobOwner)
SET @cmd = REPLACE(@cmd, '#STEPNAME#', @jobStepName)
SET @cmd = REPLACE(@cmd, '#JOBCMD#', @jobCmd02)
SET @cmd = REPLACE(@cmd, '#JOBSERVER#', @serverName)

BEGIN TRY
	EXECUTE sp_executesql @cmd
END TRY
BEGIN CATCH
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not create SQL Agent Job: ' + @jobName02 + CHAR(13) + CHAR(10) +
	'Error Number: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
	'Error Message: ' + ERROR_MESSAGE()
	RAISERROR(@msg, 10, 1) WITH NOWAIT;
	RETURN;
END CATCH

--#########################################################################
-- Generate Job 3
SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Generating job: ' + @jobName03
RAISERROR(@msg, 10, 1) WITH NOWAIT;

-- Job CMD
-- Check how many files there are
SET @jobCmd03 = 'USE [' + @dbName + '];' + CHAR(13) + CHAR(10) +
				'DECLARE @fileCount int = 0' + CHAR(13) + CHAR(10) +
				'SET @fileCount = (SELECT COUNT(1)' + CHAR(13) + CHAR(10) +
				'FROM sys.database_files a' + CHAR(13) + CHAR(10) +
				'LEFT JOIN sys.filegroups b' + CHAR(13) + CHAR(10) +
				'ON a.data_space_id = b.data_space_id' + CHAR(13) + CHAR(10) +
				'WHERE a.type = 0' + CHAR(13) + CHAR(10) +
				'AND b.name = ''''' + @fileGroup + ''''')' + CHAR(13) + CHAR(10) +
				'IF((''''' + @filegroup + ''''' = ''''PRIMARY'''') AND (@fileCount = 1))' + CHAR(13) + CHAR(10) +
				'OR' + CHAR(13) + CHAR(10) +
				'((''''' + @filegroup + ''''' != ''''PRIMARY'''') AND (@fileCount > 1))' + CHAR(13) + CHAR(10) +
				'BEGIN' + CHAR(13) + CHAR(10) +
				'RAISERROR(''''Data movement to temporary file did not succeed'''', 16 , 1) WITH NOWAIT;' + CHAR(13) + CHAR(10) +
				'RETURN;' + CHAR(13) + CHAR(10) +
				'END' + CHAR(13) + CHAR(10)

-- Add new files
SET @jobCmd03 += 'USE [master];' + CHAR(13) + CHAR(10) +
				 'BEGIN TRY' + CHAR(13) + CHAR(10)
SELECT @jobCmd03 += ISNULL(('ALTER DATABASE [' + @dbName + '] ADD FILE (NAME = N''''' + logicalName + ''''', FILENAME = N''''' + filepath + ''''', SIZE = ' + CONVERT(NVARCHAR(MAX), @newInitFileSizeMB) + 'MB, FILEGROWTH = ' + CONVERT(NVARCHAR(MAX), @newAutoGrowSizeMB) + 'MB ) TO FILEGROUP [' + @fileGroup + '];' + CHAR(13) + CHAR(10)), '')
FROM @tblTargetFilenames
ORDER BY logicalName ASC

SET @jobCmd03 += 'END TRY'  + CHAR(13) + CHAR(10) +
				 'BEGIN CATCH' + CHAR(13) + CHAR(10) +
				 'THROW;' + CHAR(13) + CHAR(10) +
				 'RETURN;' + CHAR(13) + CHAR(10) +
				 'END CATCH'  + CHAR(13) + CHAR(10) 

-- If it's primary filegroup -> shrink MDF to size
IF(@filegroup = 'PRIMARY')
BEGIN
	SET @jobCmd03 += 'DECLARE @cmd NVARCHAR(MAX)' + CHAR(13) + CHAR(10) +
					 'DECLARE @logicalName NVARCHAR(256) = (SELECT CONVERT(NVARCHAR(MAX), name) FROM master.sys.master_files WHERE type = 0 AND file_id = 1 AND database_id = DB_ID(''''' + @dbName + '''''))' + CHAR(13) + CHAR(10) +
					 'IF((SELECT size * 8 / 1024' + CHAR(13) + CHAR(10) +
					 'FROM master.sys.master_files' + CHAR(13) + CHAR(10) +
					 'WHERE database_id = DB_ID(''''' + @dbName + ''''')' + CHAR(13) + CHAR(10) +
					 'AND file_id = 1' + CHAR(13) + CHAR(10) +
					 'AND type = 0) >= ' + CONVERT(NVARCHAR(MAX), @newInitFileSizeMB) + ')'  + CHAR(13) + CHAR(10) +
					 'BEGIN' + CHAR(13) + CHAR(10) +
					 'SET @cmd = ''''USE [' + @dbName + '];''''' + CHAR(13) + CHAR(10) +
					 'SET @cmd += ''''DBCC SHRINKFILE (N'''''''''''' + @logicalName + '''''''''''' , ' + CONVERT(NVARCHAR(MAX), @newInitFileSizeMB) + ');''''' + CHAR(13) + CHAR(10) +
					 'END' + CHAR(13) + CHAR(10) +
					 'ELSE' + CHAR(13) + CHAR(10) +
					 'BEGIN' + CHAR(13) + CHAR(10) +
					 'SET @cmd = ''''USE [' + @dbName + '];''''' + CHAR(13) + CHAR(10) +
					 'SET @cmd += ''''ALTER DATABASE [' + @dbName + '] MODIFY FILE ( NAME = N'''''''''''' + @logicalName + '''''''''''', SIZE = ' + CONVERT(NVARCHAR(MAX), @newInitFileSizeMB) + 'MB );''''' + CHAR(13) + CHAR(10) +
					 'END' + CHAR(13) + CHAR(10) +
					 --Set Autogrow value
					 'SET @cmd += ''''USE [' + @dbName + '];''''' + CHAR(13) + CHAR(10) +
					 'SET @cmd += ''''ALTER DATABASE [' + @dbName + '] MODIFY FILE ( NAME = N'''''''''''' + @logicalName + '''''''''''', FILEGROWTH = ' + CONVERT(NVARCHAR(MAX), @newAutoGrowSizeMB) + 'MB );'''''+ CHAR(13) + CHAR(10)
END
-- Execute DynSQL
SET @jobCmd03 += 'EXEC sp_executesql @cmd' + CHAR(13) + CHAR(10)

--Set Autogrow all files
SET @jobCmd03 += 'USE [master];' + CHAR(13) + CHAR(10) +
				 'ALTER DATABASE [' + @dbName + '] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;' + CHAR(13) + CHAR(10) +
				 'USE [' + @dbName + '];' + CHAR(13) + CHAR(10) +
				 'ALTER DATABASE [' + @dbName + '] MODIFY FILEGROUP [' + @fileGroup + '] AUTOGROW_ALL_FILES;' + CHAR(13) + CHAR(10) +
				 'USE [master];' + CHAR(13) + CHAR(10) +
				 'ALTER DATABASE [' + @dbName + '] SET MULTI_USER;' + CHAR(13) + CHAR(10)

SET @cmd = @createJobTemplate
SET @cmd = REPLACE(@cmd, '#JOBNAME#', @jobName03)
SET @cmd = REPLACE(@cmd, '#JOBDESC#', @job03Desc)
SET @cmd = REPLACE(@cmd, '#JOBOWNER#', @jobOwner)
SET @cmd = REPLACE(@cmd, '#STEPNAME#', @jobStepName)
SET @cmd = REPLACE(@cmd, '#JOBCMD#', @jobCmd03)
SET @cmd = REPLACE(@cmd, '#JOBSERVER#', @serverName)

BEGIN TRY
	EXECUTE sp_executesql @cmd
END TRY
BEGIN CATCH
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not create SQL Agent Job: ' + @jobName03 + CHAR(13) + CHAR(10) +
	'Error Number: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
	'Error Message: ' + ERROR_MESSAGE()
	RAISERROR(@msg, 10, 1) WITH NOWAIT;
	RETURN;
END CATCH

--#########################################################################
-- Generate Job 4
SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Generating job: ' + @jobName04
RAISERROR(@msg, 10, 1) WITH NOWAIT;

-- Check if Simple mode
SET @jobCmd04 = 'IF(' + CHAR(13) + CHAR(10) +
				'(SELECT recovery_model_desc' + CHAR(13) + CHAR(10) +
				'FROM master.sys.databases' + CHAR(13) + CHAR(10) +
				'WHERE name = ''''' + @dbName + ''''') != ''''SIMPLE'''')' + CHAR(13) + CHAR(10) +
				'BEGIN' + CHAR(13) + CHAR(10) +
				'RAISERROR(''''Database needs to be in SIMPLE recovery model'''', 16, 4) WITH NOWAIT;' + CHAR(13) + CHAR(10) +
				'RETURN;'  + CHAR(13) + CHAR(10) +
				'END' + CHAR(13) + CHAR(10)

-- Shrink Operation
SET @jobCmd04 += 'USE [' + @dbName + '];' + CHAR(13) + CHAR(10) +
				 'DBCC SHRINKFILE (N''''' + @tempFileLogicalName + ''''' , EMPTYFILE);' + CHAR(13) + CHAR(10) +
				 'GO' + CHAR(13) + CHAR(10) +
				 'USE [' + @dbName + '];' + CHAR(13) + CHAR(10) +
				 'ALTER DATABASE [' + @dbName + ']  REMOVE FILE [' + @tempFileLogicalName + '];' + CHAR(13) + CHAR(10)

-- Handle Job
SET @cmd = @createJobTemplate
SET @cmd = REPLACE(@cmd, '#JOBNAME#', @jobName04)
SET @cmd = REPLACE(@cmd, '#JOBDESC#', @job04Desc)
SET @cmd = REPLACE(@cmd, '#JOBOWNER#', @jobOwner)
SET @cmd = REPLACE(@cmd, '#STEPNAME#', @jobStepName)
SET @cmd = REPLACE(@cmd, '#JOBCMD#', @jobCmd04)
SET @cmd = REPLACE(@cmd, '#JOBSERVER#', @serverName)

BEGIN TRY
	EXECUTE sp_executesql @cmd
END TRY
BEGIN CATCH
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not create SQL Agent Job: ' + @jobName04 + CHAR(13) + CHAR(10) +
	'Error Number: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
	'Error Message: ' + ERROR_MESSAGE()
	RAISERROR(@msg, 10, 1) WITH NOWAIT;
	RETURN;
END CATCH

SET @msg = 'All jobs have been created'
RAISERROR(@msg, 10, 1) WITH NOWAIT;
SET @msg = '!!! IMPORTANT !!! DO NOT RUN THIS SCRIPT AFTER YOU STARTED WITH THE PROCESS!!!'
RAISERROR(@msg, 10, 1) WITH NOWAIT;
SET @msg = 'Please ensure prior running the SQL Server Agent Jobs that the database is not a member of an Availability Group and that it is in SIMPLE recovery model'
RAISERROR(@msg, 10, 1) WITH NOWAIT;
SET @msg = 'Runn all SQL Agent Jobs (XX_SplitDataFiles_...) in sequence'
RAISERROR(@msg, 10, 1) WITH NOWAIT;
SET @msg = 'Every single job needs to finish successfully before moving on to the next job'
RAISERROR(@msg, 10, 1) WITH NOWAIT;
SET @msg = 'Use "Disk Usage" report to monitor progress (Right-click on the database --> Reports --> Standard reports --> Disk Usage --> Expand "Disk Space Used by Data Files"'
RAISERROR(@msg, 10, 1) WITH NOWAIT;

EXEC AdminDB.dbo.spr_SplitDataFiles 
	@dbName = 'LargeDB1FG', 
	@fileGroup = 'PRIMARY',
	@tempFilename  = 'D:\MSSQL\SQL16EE\data\tempfile.ndf',
	@newFilename = 'D:\MSSQL\SQL16EE\data\file_02.ndf;
					D:\MSSQL\SQL16EE\data\file_03.ndf;
					D:\MSSQL\SQL16EE\data\file_04.ndf;
					D:\MSSQL\SQL16EE\data\file_05.ndf;'

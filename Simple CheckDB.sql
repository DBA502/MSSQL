USE [master]
GO

/****** Object:  Table [dbo].[CheckDB]    Script Date: 8/24/2018 2:04:22 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CheckDB](
	[instance] [varchar](255) NOT NULL,
	[database] [varchar](255) NOT NULL,
	[size] [int] NOT NULL,
	[result] [varchar](max) NULL,
	[checkdb_type] [varchar](255) NULL,
	[data_collection_timestamp] [smalldatetime] NULL,
	[completion_time] [int] NULL,
	[last_good_dbcc] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

---------------------------------------------------------------------------------------

USE [master]
GO

/****** Object:  StoredProcedure [dbo].[SDBA_CHECKDB]    Script Date: 8/30/2018 3:45:26 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Alejandro Cobar
-- Create date: 8/22/2018
-- Description:	Runs DBCC CHECKDB ON each database and stores the output message 
-- =============================================
CREATE PROCEDURE [dbo].[SDBA_CHECKDB]
@dbSizeThreshold INT = 512000,
@force INT = 0
AS
BEGIN
	SET NOCOUNT ON;

	-- Temporal table to obtain the "Last Known Good DBCC CHECKDB" execution for each database
	IF NOT OBJECT_ID('tempdb..#DBInfo') IS NULL  
	DROP TABLE #DBInfo; 

	CREATE TABLE #DBInfo
	([ParentObject] VARCHAR(255)
	,[Object] VARCHAR(255)
	,[Field] VARCHAR(255)
	,[Value] VARCHAR(255)
	)

	-- Depending on the SQL Server version, the respective temporal table will be created to store the CHECKDB results
	DECLARE @version INT;
	SELECT @version = RIGHT(LEFT(@@VERSION,25),4);

	--Starting from SQL Server 2012, new fields were introduced to the output of DBCC CHECKDB WITH TABLERESULTS
	IF NOT OBJECT_ID('tempdb..#CheckDB_old') IS NULL  
	DROP TABLE #CheckDB_old; 

	IF NOT OBJECT_ID('tempdb..#CheckDB_new') IS NULL  
	DROP TABLE #CheckDB_new;

	IF @version >= 2012
	CREATE TABLE #CheckDB_new 
	([Error] INT 
	,[Level] INT 
	,[State] INT 
	,[MessageText] VARCHAR(MAX) 
	,[RepairLevel] INT 
	,[Status] INT 
	,[DbId] INT 
	,[DbFragId] INT 
	,[ObjectID] INT 
	,[IndexId] INT 
	,[PartitionId] INT 
	,[AllocUnitId] INT 
	,[RidDbId] INT 
	,[RidPruId] INT 
	,[File] INT 
	,[Page] INT 
	,[Slot] INT 
	,[RefDbID] INT 
	,[RefPruId] INT 
	,[RefFile] INT 
	,[RefPage] INT 
	,[RefSlot] INT 
	,[Allocation] INT);
	ELSE
	CREATE TABLE #CheckDB_old 
	([Error] INT 
	,[Level] INT 
	,[State] INT 
	,[MessageText] VARCHAR(MAX) 
	,[RepairLevel] INT 
	,[Status] INT 
	,[DbId] INT 
	,[ObjectID] INT 
	,[IndexId] INT 
	,[PartitionId] INT 
	,[AllocUnitId] INT 
	,[File] INT 
	,[Page] INT 
	,[Slot] INT 
	,[RefFile] INT 
	,[RefPage] INT 
	,[RefSlot] INT 
	,[Allocation] INT);
	
	-- We don't want to keep all the CHECKDB results here for a very long time...
	TRUNCATE TABLE master.dbo.CheckDB;

	-- Insert all the databases that will be checked
	-- Only consider those in ONLINE state and exclude the SNAPSHOTS
	INSERT INTO master.dbo.CheckDB
	SELECT CAST(SERVERPROPERTY('SERVERNAME') AS VARCHAR(255)), DB_NAME(mf.database_id), SUM(mf.size*8)/1024, NULL, NULL, NULL, NULL,NULL
	FROM sys.master_files mf
	JOIN sys.databases db ON mf.database_id = db.database_id
	WHERE mf.state_desc = 'ONLINE' AND db.source_database_id IS NULL
	GROUP BY mf.database_id;
	
	-- Prepare a cursor to have a better control of which databases where checked and which weren't
	-- A sudden server or instance reboot might affect this whole process...
	DECLARE @db VARCHAR(255);

	DECLARE checkdb_cursor CURSOR FOR 
	SELECT [database] 
	FROM master.dbo.CheckDB
	WHERE result IS NULL;

	OPEN checkdb_cursor  
	FETCH NEXT FROM checkdb_cursor INTO @db
	
	WHILE @@FETCH_STATUS = 0  
	BEGIN  
		DECLARE @startTime DATETIME = GETDATE();
		DECLARE @endTime DATETIME;

      	DECLARE @databaseSize INT;	
		SELECT @databaseSize = size FROM master.dbo.CheckDB WHERE [database] = @db;
	
		IF @databaseSize <= @dbSizeThreshold OR @force = 1
		BEGIN
			IF @version >= 2012
			BEGIN
				INSERT INTO #CheckDB_new 
				([Error], [Level], [State], [MessageText], [RepairLevel], 
				[Status], [DbId], [DbFragId], [ObjectID], [IndexId], [PartitionId], 
				[AllocUnitId], [RidDbId], [RidPruId], [File], [Page], [Slot], [RefDbID], 
				[RefPruId], [RefFile], [RefPage], [RefSlot], [Allocation]) 
				EXEC ('DBCC CHECKDB('+@db+') WITH TABLERESULTS');
				
				SET @endTime = GETDATE();

				UPDATE master.dbo.CheckDB
				SET result = MessageText, checkdb_type = 'FULL', data_collection_timestamp = GETDATE(), completion_time = DATEDIFF(ss, @startTime, @endTime)
				FROM #CheckDB_new
				WHERE [Error] = 8989 AND [database] = @db;

				IF @db = 'master'
				INSERT INTO master.dbo.CheckDB 
				SELECT CAST(SERVERPROPERTY('SERVERNAME') AS VARCHAR(255)), 'mssqlsystemresource', (SELECT CONVERT(DECIMAL(10,2),SUM(size / 1024.0)) AS 'size' FROM sys.sysaltfiles WHERE DBID = 32767), MessageText, 'FULL', GETDATE(), completion_time = DATEDIFF(ss, @startTime, @endTime), NULL
				FROM #CheckDB_new
				WHERE [Error] = 8989 AND DbId = 32767;

				TRUNCATE TABLE #CheckDB_new;
			END;
			ELSE
			BEGIN
				INSERT INTO #CheckDB_old
				([Error], [Level], [State], [MessageText], [RepairLevel], 
				[Status], [DbId], [ObjectID], [IndexId], [PartitionId], 
				[AllocUnitId], [File], [Page], [Slot], [RefFile], [RefPage], [RefSlot], [Allocation]) 
				EXEC ('DBCC CHECKDB('+@db+') WITH TABLERESULTS');

				SET @endTime = GETDATE();

				UPDATE master.dbo.CheckDB
				SET result = MessageText, checkdb_type = 'FULL', data_collection_timestamp = GETDATE(), completion_time = DATEDIFF(ss, @startTime, @endTime)
				FROM #CheckDB_old
				WHERE [Error] = 8989 AND [database] = @db;

				IF @db = 'master'
				INSERT INTO master.dbo.CheckDB 
				SELECT CAST(SERVERPROPERTY('SERVERNAME') AS VARCHAR(255)), 'mssqlsystemresource', (SELECT CONVERT(DECIMAL(10,2),SUM(size / 1024.0)) AS 'size' FROM sys.sysaltfiles WHERE DBID = 32767), MessageText, 'FULL', GETDATE(), completion_time = DATEDIFF(ss, @startTime, @endTime), NULL
				FROM #CheckDB_old
				WHERE [Error] = 8989 AND DbId = 32767;

				TRUNCATE TABLE #CheckDB_old;
			END;
		END;
		ELSE
		BEGIN
			IF @version >= 2012
			BEGIN
				INSERT INTO #CheckDB_new 
				([Error], [Level], [State], [MessageText], [RepairLevel], 
				[Status], [DbId], [DbFragId], [ObjectID], [IndexId], [PartitionId], 
				[AllocUnitId], [RidDbId], [RidPruId], [File], [Page], [Slot], [RefDbID], 
				[RefPruId], [RefFile], [RefPage], [RefSlot], [Allocation]) 
				EXEC ('DBCC CHECKDB('+@db+') WITH TABLERESULTS, PHYSICAL_ONLY');
				
				SET @endTime = GETDATE();

				UPDATE master.dbo.CheckDB
				SET result = MessageText, checkdb_type = 'PHYSICAL ONLY', data_collection_timestamp = GETDATE(), completion_time = DATEDIFF(ss, @startTime, @endTime)
				FROM #CheckDB_new
				WHERE [Error] = 8989 AND [database] = @db;

				IF @db = 'master'
				INSERT INTO master.dbo.CheckDB 
				SELECT CAST(SERVERPROPERTY('SERVERNAME') AS VARCHAR(255)), 'mssqlsystemresource', (SELECT CONVERT(DECIMAL(10,2),SUM(size / 1024.0)) AS 'size' FROM sys.sysaltfiles WHERE DBID = 32767), MessageText, 'PHYSICAL ONLY', GETDATE(), completion_time = DATEDIFF(ss, @startTime, @endTime), NULL
				FROM #CheckDB_new
				WHERE [Error] = 8989 AND DbId = 32767;

				TRUNCATE TABLE #CheckDB_new;
			END;
			ELSE
			BEGIN
				INSERT INTO #CheckDB_old
				([Error], [Level], [State], [MessageText], [RepairLevel], 
				[Status], [DbId], [ObjectID], [IndexId], [PartitionId], 
				[AllocUnitId], [File], [Page], [Slot], [RefFile], [RefPage], [RefSlot], [Allocation]) 
				EXEC ('DBCC CHECKDB('+@db+') WITH TABLERESULTS, PHYSICAL_ONLY');

				SET @endTime = GETDATE();

				UPDATE master.dbo.CheckDB
				SET result = MessageText, checkdb_type = 'PHYSICAL ONLY', data_collection_timestamp = GETDATE(), completion_time = DATEDIFF(ss, @startTime, @endTime)
				FROM #CheckDB_old
				WHERE [Error] = 8989 AND [database] = @db;

				IF @db = 'master'
				INSERT INTO master.dbo.CheckDB 
				SELECT CAST(SERVERPROPERTY('SERVERNAME') AS VARCHAR(255)), 'mssqlsystemresource', (SELECT CONVERT(DECIMAL(10,2),SUM(size / 1024.0)) AS 'size' FROM sys.sysaltfiles WHERE DBID = 32767), MessageText, 'PHYSICAL ONLY', GETDATE(), completion_time = DATEDIFF(ss, @startTime, @endTime), NULL
				FROM #CheckDB_old
				WHERE [Error] = 8989 AND DbId = 32767;

				TRUNCATE TABLE #CheckDB_old;
			END;
		END;
		
		-- Get the information for the "Last Known Good DBCC CHECKDB" execution 
		INSERT INTO #DBInfo ([ParentObject], [Object], [Field], [Value]) 
	    EXEC ('DBCC DBINFO('+@db+') WITH TABLERESULTS');

		UPDATE master.dbo.CheckDB
		SET last_good_dbcc = [Value]
		FROM #DBInfo
		WHERE [Field] = 'dbi_dbccLastKnownGood' AND [database] = @db;

		UPDATE master.dbo.CheckDB SET last_good_dbcc = (SELECT last_good_dbcc FROM master.dbo.CheckDB WHERE [database] = 'master');

		TRUNCATE TABLE #DBInfo; 

		FETCH NEXT FROM checkdb_cursor INTO @db 
	END

	CLOSE checkdb_cursor  
	DEALLOCATE checkdb_cursor  
	
	-- Drop whichever temporal table was created
	IF NOT OBJECT_ID('tempdb..#CheckDB_old') IS NULL  
	DROP TABLE #CheckDB_old; 

	IF NOT OBJECT_ID('tempdb..#CheckDB_new') IS NULL
	DROP TABLE #CheckDB_new; 

	IF NOT OBJECT_ID('tempdb..#DBInfo') IS NULL  
	DROP TABLE #DBInfo; 
END
GO

---------------------------------------------------------------------------------------

USE [msdb]
GO

/****** Object:  Job [Database Consistency Check]    Script Date: 8/28/2018 5:53:06 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 8/28/2018 5:53:06 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'SDBA - Database Consistency Check', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Runs a DBCC CHECKDB operation against all the databases in the SQL Server instance', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [CheckDB]    Script Date: 8/28/2018 5:53:07 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'CheckDB', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC SDBA_CHECKDB', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'CHECKDB Execution', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=64, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20180822, 
		@active_end_date=99991231, 
		@active_start_time=200000, 
		@active_end_time=235959, 
		@schedule_uid=N'426bbf27-fd1d-467a-b546-7511f9860a49'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO
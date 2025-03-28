BEGIN
	INSERT INTO DBADMINDATA.ARGOCDAPPS(AID, 
										APPID, 
										APPNAMESPACE, 
										APPPROJECT, 
										APPSERVER, 
										APPPATH, 
										APPREVISION, 
										APPURL, 
										AHEALTH, 
										ATS, 
										ASYNC, 
										CREATED)
					VALUES(:aid, 
							:appid, 
							:appnamespace, 
							:appproject, 
							:appserver, 
							:apppath, 
							:apprevision, 
							:appurl, 
							:ahealth, 
							:ats, 
							:sync_status, 
							:created);
EXCEPTION WHEN dup_val_on_index THEN
	UPDATE DBADMINDATA.ARGOCDAPPS
		SET APPNAMESPACE = :appnamespace,
			APPPROJECT = :appproject,
			APPSERVER = :appserver,
			APPPATH = :apppath,
			APPREVISION = :apprevision,
			APPURL = :appurl,
			AHEALTH = NVL(:ahealth, ahealth),
			ATS = NVL(:ats, ats),
			ASYNC = NVL(:sync_status, async),
			CREATED = NVL(:created, created)
	WHERE AID = :aid 
		AND APPID = :appid;
END;
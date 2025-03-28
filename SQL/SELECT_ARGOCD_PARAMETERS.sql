SELECT AHOSTNAME||':'||to_char(APORT)||APATH||'/stream/applications' AS PATH,
			ATOKEN AS TOKEN
	FROM ARGOCDINFO 
	WHERE AID = :argoid
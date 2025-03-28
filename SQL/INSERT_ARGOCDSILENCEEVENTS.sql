INSERT INTO ARGOCDSILENCEEVENTS(EVENTID, 
								EVENTTS, 
								ARGOID, 
								APPID)
			VALUES(ARGOSILENCEEVENTSSEQ.NEXTVAL,
					:eventts, 
					:argoid, 
					:appid)
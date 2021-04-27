CREATE TABLE IF NOT EXISTS cdn_stats (
		id text PRIMARY KEY,
		date text,
		stream_id text,
		unique_users bigint,
		total_views bigint,
		total_cs_bytes bigint,
		total_sc_bytes bigint,
		total_file_size bigint
	 );
INSERT INTO cdn_stats (id, date,stream_id,unique_users,total_views,total_cs_bytes,total_sc_bytes,total_file_size) 
		VALUES ('2021-04-16_z554x7u0m9p7gi', '2021-04-16', 'z554x7u0m9p7gi', 6, 9, 4423, 7065, 531)
		ON CONFLICT (id) DO UPDATE 
		SET date = '2021-04-16', 
			stream_id = 'z554x7u0m9p7gi',
			unique_users = 6,
			total_views = 9
			total_cs_bytes = 4423,
			total_sc_bytes = 7065,
			total_file_size = 531;
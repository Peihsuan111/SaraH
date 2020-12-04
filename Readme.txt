Setup steps:

*revise
*unsolved
	
	1. Load data
		//in bat server environment//

		order detail:(revise the parameter: input_month)
		-load by quarterly
		# python3 friDay_Dashboard_quarterly.py >>/home/sara/Files/log_dashboard.txt 
	
		store:
		-reload all data
		# python3 friDay_Dashboard_Store.py >>/home/sara/Files/log_store.txt 

		bonus:
		-reload all data(unsolved:seperate by quarterly, but index is orderId. Unsolved problem)
		# python3 friDay_Dashboard_bonus.py >>/home/sara/Files/log_bonus.txt 
		
		label:(*unsolved: 同一個tag_family有兩個tag_id ex."orderId" :20201027732506. 瀏覽來源: 其他推薦, 廣宣) 
		-load in order's tag_id, tag_name, tag_family_name
		# python friDay+Dashboard_label.py >>/home/sara/Files/log_label.txt

	2. Copy files from barServer to local(on local)
		# scp -i batserverkey.key sara@104.199.216.123:/file/to/send/remote /file/to/receive/local/

	3. Files Connect to Tableau



	PostgreSQL setup
	1. create table:(orderDetail, store, bonus)
		cd file/to/sql/location
		psql -h 192.168.68.111 -d bat -U huang < friDayTable.sql #on Ubuntu
	
	2. Temp stop to load in PostgreSQL...(Tableau can not connect to PostgreSQL)






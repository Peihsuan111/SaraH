CREATE TABLE friDay_orderDetail(
	orderId	text    NOT NULL,
	dealId	text,
	dateWeek	text,
	date	text,
	dateTime		text,
	storeId		text,
	new_storeId	text,
	memuid	text,
	yearRange		text,
	firstMonthBuy	text,
	financeType	text,
	firstYearBuy	text,
	sex	text,
	device	text,
	channelId3	text,
	sales	text,
	cost	text,
	profit text,	
	coupon	text,
	discountCode text,	
	fCoin	text,
	fCoinReturn	text,
	happyGo	text
	);

CREATE TABLE friDay_store(
	storeId	text    NOT NULL,
	name	text,
	startDate	text,
	endDate	text,
	status		text,
	type		text,
	cart		text,
	saleStore	text
	);

CREATE TABLE friDay_bonus(
	orderId	text    NOT NULL,
	update_date	text,
	BONUS	text,
	CHANNELID	text
	);



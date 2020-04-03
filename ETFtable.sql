CREATE TABLE ETF(
    name	text	NOT NULL,
    datetime    text    NOT NULL,
    vol_share	text,
    vol_num	text,
    vol_amount	text,	
    open        text,
    high        text,
    low         text,
    close       text,
    spread	text,
    lastBuy_amount      text,
    lastBuy_vol	text,
    lastSell_amount      text,
    lastSell_vol	text,
    PEratio	text,
	PRIMARY KEY (name, datetime)
	
);

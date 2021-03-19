CREATE TABLE stock (
    id SERIAL PRIMARY KEY, 
    symbol TEXT NOT NULL,
    full_name TEXT NOT NULL,
    exchange TEXT NOT NULL,
    is_etf BOOLEAN NOT NULL
);
CREATE TABLE etf_holding (
    etf_id INTEGER NOT NULL,
    holding_id INTEGER NOT NULL,
    dt DATE NOT NULL,
    num_shares NUMERIC NOT NULL,
    percent_held NUMERIC NOT NULL, 
    PRIMARY KEY (etf_id, holding_id, dt), -- we want to uniquely identify one of the etf holdings in the stock tables? We need a COMPOUND PRIMARY KEY 
    CONSTRAINT fk_etf FOREIGN KEY (etf_id) REFERENCES stock (id), 
    CONSTRAINT fk_holding FOREIGN KEY (holding_id) REFERENCES stock (id)
);

CREATE TABLE stock_price (
    stock_id INTEGER NOT NULL, 
    dt TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    open NUMERIC NOT NULL,    
    high NUMERIC NOT NULL, 
    low NUMERIC NOT NULL, 
    close NUMERIC NOT NULL, 
    volume NUMERIC NOT NULL, 
    PRIMARY KEY (stock_id, dt),
    CONSTRAINT fk_stock FOREIGN KEY (stock_id) REFERENCES stock(id)
 
);

CREATE INDEX ON stock_price (stock_id, dt DESC); -- Optimize your ability to find a record (improves speed & efficiency)

SELECT create_hypertable('stock_price', 'dt'); -- in timescale, a hypertable is an abstraction that allows you to take advantage of timscales functions for tables with TIMESTAMPS
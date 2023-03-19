//! A client that manages calls to the Binance API.


// Dependencies
// stdlib
use std::collections::HashMap;

// external
use reqwest::Client;
use serde::{Deserialize, Serialize};
use polars::prelude::*;

// Types
type Kline = (i64, String, String, String, String, String, i64, String, i32, String, String, String);


struct BinanceClient {
    client: Client,
    exchange: Exchange,
}

impl BinanceClient {
    async fn new() -> Self {
        let client = Client::new();
        let exchange = get_exchange(&client).await.unwrap();

        Self {
            client,
            exchange,
        }
    }

    async fn get_symbol_data(&self, symbol: &str, interval: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.exchange.symbols.get(symbol).ok_or("Symbol not found")?;

        let klines = &self
            .client
            .get("https://api.binance.com/api/v3/klines")
            .query(&[("symbol", "BTCUSDT"), ("interval", "1m")])
            .send()
            .await?
            .json::<Vec<Kline>>()
            .await?;

        Ok(())
    }
}


#[derive(Debug, Deserialize, Serialize)]
struct ExchangeData {
    timezone: String,
    #[serde(rename = "serverTime")]
    server_time: u64,
    symbols: Vec<Symbol>,
}

#[derive(Debug)]
struct Exchange {
    timezone: String,
    server_time: u64,
    symbols: HashMap<String, Symbol>,
}

impl From<ExchangeData> for Exchange {
    fn from(data: ExchangeData) -> Self {
        let mut symbols = HashMap::new();

        for symbol in data.symbols {
            symbols.insert(symbol.symbol.clone(), symbol);
        }

        Self {
            timezone: data.timezone,
            server_time: data.server_time,
            symbols,
        }
    }
}


#[derive(Debug, Deserialize, Serialize)]
struct Symbol {
    symbol: String,
    status: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "baseAssetPrecision")]
    base_asset_precision: u8,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    #[serde(rename = "quotePrecision")]
    quote_precision: u8,
    #[serde(rename = "orderTypes")]
    order_types: Vec<String>,
    #[serde(rename = "icebergAllowed")]
    iceberg_allowed: bool,
    #[serde(rename = "ocoAllowed")]
    oco_allowed: bool,
    #[serde(rename = "quoteOrderQtyMarketAllowed")]
    quote_order_qty_market_allowed: bool,
    #[serde(rename = "isSpotTradingAllowed")]
    is_spot_trading_allowed: bool,
    #[serde(rename = "isMarginTradingAllowed")]
    is_margin_trading_allowed: bool,
    permissions: Vec<String>,
}


async fn get_all_symbols(client: &Client) -> Result<ExchangeData, Box<dyn std::error::Error>>{
    let exchange_data = client
        .get("https://api.binance.com/api/v3/exchangeInfo")
        .send()
        .await?
        .json::<ExchangeData>()
        .await?;

    Ok(exchange_data)
}


async fn get_exchange(client: &Client) -> Result<Exchange, Box<dyn std::error::Error>> {
    let exchange_data = get_all_symbols(client).await?;
    Ok(exchange_data.into())
}

fn klines_to_columns(klines: Vec<Kline>) -> Vec<Series> {
    let n_rows = klines.len();

    let mut open_time: Vec<i64> = Vec::with_capacity(n_rows);
    let mut close_time: Vec<i64> = Vec::with_capacity(n_rows);
    let mut open: Vec<f32> = Vec::with_capacity(n_rows);
    let mut high: Vec<f32> = Vec::with_capacity(n_rows);
    let mut low: Vec<f32> = Vec::with_capacity(n_rows);
    let mut close: Vec<f32> = Vec::with_capacity(n_rows);
    let mut volume: Vec<f32> = Vec::with_capacity(n_rows);

    for kline in klines {
        let (_open_time, _open, _high, _low, _close, _volume, _close_time, _, _, _, _, _) = kline;

        open_time.push(_open_time);
        close_time.push(_close_time);
        open.push(_open.parse().unwrap());
        high.push(_high.parse().unwrap());
        low.push(_low.parse().unwrap());
        close.push(_close.parse().unwrap());
        volume.push(_volume.parse().unwrap());
    }

    vec![
        Series::new("open_time", open_time),
        Series::new("close_time", close_time),
        Series::new("open", open),
        Series::new("high", high),
        Series::new("low", low),
        Series::new("close", close),
        Series::new("volume", volume),
    ]
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_all_symbols() {
        let client = Client::new();
        let symbols = get_all_symbols(&client).await.unwrap();

        println!("{:#?}", symbols);
    }

    #[tokio::test]
    async fn test_get_exchange() {
        let client = Client::new();
        let exchange = get_exchange(&client).await.unwrap();

        println!("{:#?}", exchange);
    }

    #[tokio::test]
    async fn test_get_kline() -> Result<(), Box<dyn std::error::Error>> {
        let client = Client::new();

        let info = client
            .get("https://api.binance.com/api/v3/klines")
            .query(&[("symbol", "BTCUSDT"), ("interval", "1m")])
            .send()
            .await?
            .json::<Vec<Kline>>()
            .await?;

        let cols = klines_to_columns(info);

        let df = DataFrame::new(cols)?;

        println!("{:#?}", df);

        Ok(())
    }
}

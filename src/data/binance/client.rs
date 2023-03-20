//! A client that manages calls to the Binance API.


// Dependencies
// stdlib
use std::collections::HashMap;

// external
use thiserror::Error;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use polars::prelude::*;

// Types
type Kline = (i64, String, String, String, String, String, i64, String, i32, String, String, String);

struct Klines(Vec<Kline>);


impl IntoIterator for Klines {
    type Item = Kline;
    type IntoIter = std::vec::IntoIter<Kline>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl From<Vec<Kline>> for Klines {
    fn from(klines: Vec<Kline>) -> Self {
        Self(klines)
    }
}


impl TryFrom<Klines> for DataFrame {
    type Error = BinanceError;

    fn try_from(klines: Klines) -> Result<Self, Self::Error> {
        let n_rows = klines.0.len();

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

        let cols = vec![
            Series::new("open_time", open_time),
            Series::new("close_time", close_time),
            Series::new("open", open),
            Series::new("high", high),
            Series::new("low", low),
            Series::new("close", close),
            Series::new("volume", volume),
        ];

        Ok(DataFrame::new(cols)?)
    }
}

#[derive(Debug, Error)]
pub enum BinanceError {
    #[error("Symbol not found: {0}")]
    SymbolNotFound(String),

    #[error("Interval not found")]
    IntervalNotFound,

    #[error("Error executing request: {0}")]
    RequestFailure(#[from] reqwest::Error),

    #[error("Error parsing response: {0}")]
    ToDataFrameError(#[from] PolarsError),
}

enum Interval {
    Minutes(u8),
    Hours(u8),
    Days(u8),
    Weeks(u8),
    Months(u8),
}


impl TryFrom<Interval> for &'static str {
    type Error = BinanceError;

    fn try_from(interval: Interval) -> Result<Self, Self::Error> {
        match interval {
            Interval::Minutes(1) => Ok("1m"),
            Interval::Minutes(3) => Ok("3m"),
            Interval::Minutes(5) => Ok("5m"),
            Interval::Minutes(15) => Ok("15m"),
            Interval::Minutes(30) => Ok("30m"),
            Interval::Hours(1) => Ok("1h"),
            Interval::Hours(2) => Ok("2h"),
            Interval::Hours(4) => Ok("4h"),
            Interval::Hours(6) => Ok("6h"),
            Interval::Hours(8) => Ok("8h"),
            Interval::Hours(12) => Ok("12h"),
            Interval::Days(1) => Ok("1d"),
            Interval::Days(3) => Ok("3d"),
            Interval::Weeks(1) => Ok("1w"),
            Interval::Months(1) => Ok("1M"),
            _ => Err(BinanceError::IntervalNotFound),
        }
    }
}


struct BinanceClient {
    client: Client,
    exchange: Exchange,
}

impl BinanceClient {
    async fn new() -> Result<Self, BinanceError> {
        let client = Client::new();

        // Check connectivity
        client
            .get("https://api.binance.com/api/v3/ping")
            .send()
            .await?
            .error_for_status()?;

        let exchange = get_exchange(&client).await?;

        Ok(
            Self {
                client,
                exchange,
            }
        )
    }

    async fn get_symbol_data(
        &self,
        symbol: &str,
        interval: Interval,
        from: DateTime<Utc>
    ) -> Result<DataFrame, BinanceError> {
        self.exchange.symbols.get(symbol).ok_or(BinanceError::SymbolNotFound(symbol.to_owned()))?;

        let interval_str = interval.try_into()?;

        let klines: Klines = self
            .client
            .get("https://api.binance.com/api/v3/klines")
            .query(&[("symbol", symbol), ("interval", interval_str), ("limit", "1000")])
            .send()
            .await?
            .json::<Vec<Kline>>()
            .await?
            .into();

        Ok(klines.try_into()?)
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


async fn get_all_symbols(client: &Client) -> Result<ExchangeData, BinanceError>{
    let exchange_data = client
        .get("https://api.binance.com/api/v3/exchangeInfo")
        .send()
        .await?
        .json::<ExchangeData>()
        .await?;

    Ok(exchange_data)
}


async fn get_exchange(client: &Client) -> Result<Exchange, BinanceError> {
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
    async fn test_get_frame() -> Result<(), BinanceError> {
        let client = BinanceClient::new().await?;

        let frame = client.get_symbol_data("BTCUSDT", Interval::Minutes(1), Utc::now()).await?;

        println!("{:#?}", frame);

        Ok(())
    }

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

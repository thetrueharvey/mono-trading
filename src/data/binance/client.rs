//! A client that manages calls to the Binance API.


// Dependencies
// stdlib

// external
use reqwest::Client;
use serde::{Deserialize};


#[derive(Debug, Deserialize)]
struct ExchangeInfo {
    timezone: String,
    #[serde(rename = "serverTime")]
    server_time: u64,
    symbols: Vec<Symbol>,
}

#[derive(Debug, Deserialize)]
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



async fn get_all_symbols() -> Result<ExchangeInfo, Box<dyn std::error::Error>>{
    let client = Client::new();
    let info = client
        .get("https://api.binance.com/api/v1/exchangeInfo")
        .send()
        .await?
        .json::<ExchangeInfo>()
        .await?;
    
    Ok(info)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_all_symbols() {
        let symbols = get_all_symbols().await.unwrap();
        
        println!("{:#?}", symbols);
    }
}

package com.elad.storm.example;

import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

import java.io.IOException;

/**
 * Created by eladw on 12/21/17.
 */
public class TestYF {

    public static void main(String[] args) {
        try {
            StockQuote quote = YahooFinance.get("MSFT").getQuote();
            System.out.println("quote: " + quote);
        } catch (Exception e) {
            System.out.println("Failure " + e);
        }

    }

}

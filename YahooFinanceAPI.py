# -*- coding: utf-8 -*-
"""
Created on Sat Mar 28 15:26:07 2020

@author: Federico
"""
  
'''
Yahoo! Earnings Calendar scraper
'''
import datetime
import json
import logging
import requests
import time
from datetime import datetime as dt
import pandas as pd
import yfinance as yf

BASE_URL = 'https://finance.yahoo.com/calendar/earnings'
BASE_STOCK_URL = 'https://finance.yahoo.com/quote'
RATE_LIMIT = 2000.0
SLEEP_BETWEEN_REQUESTS_S = 60 * 60 / RATE_LIMIT
OFFSET_STEP = 100

# Logging config
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.ERROR)


class YahooEarningsCalendar(object):
    """
    This is the class for fetching earnings data from Yahoo! Finance
    """

    def __init__(self, delay=SLEEP_BETWEEN_REQUESTS_S):
        self.delay = delay

    def _get_data_dict(self, url):
        time.sleep(self.delay)
        page = requests.get(url)
        page_content = page.content.decode(encoding='utf-8', errors='strict')
        page_data_string = [row for row in page_content.split(
            '\n') if row.startswith('root.App.main = ')][0][:-1]
        page_data_string = page_data_string.split('root.App.main = ', 1)[1]
        return json.loads(page_data_string)

    def get_next_earnings_date(self, symbol):
        """Gets the next earnings date of symbol
        Args:
            symbol: A ticker symbol
        Returns:
            Unix timestamp of the next earnings date
        Raises:
            Exception: When symbol is invalid or earnings date is not available
        """
        url = '{0}/{1}'.format(BASE_STOCK_URL, symbol)
        try:
            page_data_dict = self._get_data_dict(url)
            return page_data_dict['context']['dispatcher']['stores']['QuoteSummaryStore']['calendarEvents']['earnings']['earningsDate'][0]['raw']
        except:
            raise Exception('Invalid Symbol or Unavailable Earnings Date')
    
    def get_next_earnings_info(self, ticker):
        """Gets the next earnings date of symbol
        Args:
            symbol: A ticker symbol
        Returns:
            Unix timestamp of the next earnings date
        Raises:
            Exception: When symbol is invalid or earnings date is not available
        """
        url = '{0}/{1}'.format(BASE_STOCK_URL, ticker)
        try:
            page_data_dict = self._get_data_dict(url)
            # get only the information that we want
            page_earnings_info = page_data_dict['context']['dispatcher']['stores']['QuoteSummaryStore']['calendarEvents']['earnings']
            # add the ticker
            page_earnings_info['ticker'] = ticker
            #convert earningsDate field to UTC creating a new field
            page_earnings_info['earningsDateUTC'] = dt.utcfromtimestamp(int(page_earnings_info['earningsDate'][0]['raw'])).strftime('%Y-%m-%d %H:%M:%S')
            return page_earnings_info
        except:
            raise Exception('Invalid Symbol or Unavailable Earnings Date')

    def get_next_stock_info(self, ticker):
        """Gets the next earnings date of symbol
        Args:
            symbol: A ticker symbol
        Returns:
            Unix timestamp of the next earnings date
        Raises:
            Exception: When symbol is invalid or earnings date is not available
        """
        url = '{0}/{1}'.format(BASE_STOCK_URL, ticker)
        try:
            page_data_dict = self._get_data_dict(url)
            # get only the information that we want
            page_earnings_info = page_data_dict['context']['dispatcher']['stores']['QuoteSummaryStore']
            # add the ticker
            page_earnings_info['ticker'] = ticker
            return page_earnings_info
        except:
            raise Exception('Invalid Symbol or Unavailable Earnings Date')
            
            
    def earnings_on(self, date, offset=0, count=1):
        """Gets earnings calendar data from Yahoo! on a specific date.
        Args:
            date: A datetime.date instance representing the date of earnings data to be fetched.
            offset: Position to fetch earnings data from.
            count: Total count of earnings on date.
        Returns:
            An array of earnings calendar data on date given. E.g.,
            [
                {
                    "ticker": "AMS.S",
                    "companyshortname": "Ams AG",
                    "startdatetime": "2017-04-23T20:00:00.000-04:00",
                    "startdatetimetype": "TAS",
                    "epsestimate": null,
                    "epsactual": null,
                    "epssurprisepct": null,
                    "gmtOffsetMilliSeconds": 72000000
                },
                ...
            ]
        Raises:
            TypeError: When date is not a datetime.date object.
        """
        if offset >= count:
            return []

        if not isinstance(date, datetime.date):
            raise TypeError(
                'Date should be a datetime.date object')
        date_str = date.strftime('%Y-%m-%d')
        logger.debug('Fetching earnings data for %s', date_str)
        dated_url = '{0}?day={1}&offset={2}&size={3}'.format(
            BASE_URL, date_str, offset, OFFSET_STEP)
        page_data_dict = self._get_data_dict(dated_url)
        stores_dict = page_data_dict['context']['dispatcher']['stores']
        earnings_count = stores_dict['ScreenerCriteriaStore']['meta']['total']

        # Recursively fetch more earnings on this date
        new_offset = offset + OFFSET_STEP
        more_earnings = self.earnings_on(date, new_offset, earnings_count)
        curr_offset_earnings = stores_dict['ScreenerResultsStore']['results']['rows']

        return curr_offset_earnings + more_earnings

    def earnings_between(self, from_date, to_date):
        """Gets earnings calendar data from Yahoo! in a date range.
        Args:
            from_date: A datetime.date instance representing the from-date (inclusive).
            to_date: A datetime.date instance representing the to-date (inclusive).
        Returns:
            An array of earnigs calendar data of date range. E.g.,
            [
                {
                    "ticker": "AMS.S",
                    "companyshortname": "Ams AG",
                    "startdatetime": "2017-04-23T20:00:00.000-04:00",
                    "startdatetimetype": "TAS",
                    "epsestimate": null,
                    "epsactual": null,
                    "epssurprisepct": null,
                    "gmtOffsetMilliSeconds": 72000000
                },
                ...
            ]
        Raises:
            ValueError: When from_date is after to_date.
            TypeError: When either from_date or to_date is not a datetime.date object.
        """
        if from_date > to_date:
            raise ValueError(
                'From-date should not be after to-date')
        if not (isinstance(from_date, datetime.date) and
                isinstance(to_date, datetime.date)):
            raise TypeError(
                'From-date and to-date should be datetime.date objects')
        earnings_data = []
        current_date = from_date
        delta = datetime.timedelta(days=1)
        while current_date <= to_date:
            earnings_data += self.earnings_on(current_date)
            current_date += delta
        return earnings_data
    
    # function to flat json file structure
    def flatten_json(self,y):
        """
        This function is used in order to flat the
        json structure of the call
        """
        out = {}
    
        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '_')
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '_')
                    i += 1
            else:
                out[name[:-1]] = x
    
        flatten(y)
        return out


#############################
####### MAIN ###############
#############################

if __name__ == '__main__':  # pragma: no cover

    Yec = YahooEarningsCalendar()
    #Yec.earnings_on(date_from)
    date_from = datetime.datetime.strptime(
        'Apr 1 2020  10:00AM', '%b %d %Y %I:%M%p')
    date_to = datetime.datetime.strptime(
        'Apr 2 2020  1:00PM', '%b %d %Y %I:%M%p')
    Yec.earnings_between(date_from, date_to)
    # Format as Dataframe
    DataframeCalendar = pd.DataFrame(Yec.earnings_between(date_from, date_to))
    # print out
    #DataframeCalendar.to_csv(r"C:\Users\Federico\Desktop\CalendarEarningsYahooFinance.csv")
    # Returns the next earnings date of BOX in Unix timestamp
    #print(yec.get_next_earnings_date('box'))
    
    ##### Return next earnings date converted to UTC timestamp #####
    # AAPL - Apple
    dt.utcfromtimestamp(int(Yec.get_next_earnings_date('AAPL'))).strftime('%Y-%m-%d %H:%M:%S')
    # L'Oreal - OR.PA
    dt.utcfromtimestamp(int(Yec.get_next_earnings_date('OR.PA'))).strftime('%Y-%m-%d %H:%M:%S')
    
    # European ticker to download
    # insert tickers with the list
    TickerList = ["SAP.SG","MC.PA","ALV.DE","SIE.DE","ULVR.L",
                  "OR.PA","BAYN.DE","ENEL.MI","ADS.DE","CS.PA",
                  "AIR.PA","BNP.PA","PHIA.AS","ISP.MI","DAI.DE",
                  "ENI.MI","VOW.DE","CRH.L","BMW.DE"]
    # output dataframe
    OutputList = pd.DataFrame()
    
    # extract data
    for symbol in TickerList:
        try:
            SymbolInfo = Yec.get_next_earnings_info(ticker=symbol)
            # flat the json structure
            SymbolInfoFlatten = Yec.flatten_json(y=SymbolInfo)
            # concat
            if OutputList.empty:
                OutputList = pd.DataFrame([SymbolInfoFlatten])
            else:
                SymbolInfoFlatten = pd.DataFrame([SymbolInfoFlatten])
                OutputList = OutputList.append(SymbolInfoFlatten,ignore_index=True)
        except Exception as Error:
            print(str(Error)+" for ticker "+symbol)

    # print out
    #OutputList.to_csv(r"C:\Users\Federico\Desktop\CalendarEarningsYahooFinanceEuropean.csv")
    
    ##################################################################
    ######## Get all the information available for one ticker ########  
    ##################################################################
    
    AAPlInfo = Yec.get_next_stock_info(ticker='AAPL')    
    # flatten the json file
    AAPlInfoFlatten = Yec.flatten_json(AAPlInfo)
    # convert to series, dataframe has to many columns
    AAPlInfoSeries = pd.Series(AAPlInfoFlatten).to_frame('Values')
    # print out
    #AAPlInfoSeries.to_csv(r"C:\Users\Federico\Desktop\AAPLInfos.csv")
    
    ##################
    ## try yfinance ##
    ##################
    
    data = yf.download("SPY AAPL", start="2017-01-01", end="2017-04-30")
    # try msft
    msft = yf.Ticker("MSFT")
    msft.info
    # try to get all the history --> "max"
    msft.history(period='max')
    
    sep = yf.Ticker("ES=F")
    sep.history(period='max')

    yf.Ticker("GB00B28B7B81.L").history(period='max')
    yf.Ticker("IE0004345025.IR").history(period='max')
    yf.Ticker("XDWH.MI").history(period='max')
    yf.Ticker("DGTL.MI").history(period='max')
    yf.Ticker("ITAMID.MI").history(period='max')

    
    
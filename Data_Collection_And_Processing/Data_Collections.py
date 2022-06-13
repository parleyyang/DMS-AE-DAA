import pandas as pd
from pandas import DataFrame as df
from pandas import read_csv as rc
from fredapi import Fred
import numpy as np
import yfinance as yf
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm


class DataCollection:

    '''
    Description:

    This class implements web scraping for financial data via third-party software/APIs.

    There are three categories of data we need to import:

            - VIX Futures Contracts from CBOE. This is done via web scraping in BeautifulSoup.
            - The S&P 500, VIX Index, Nasdaq100, DJIA30 indices from the Yahoo Finance API
            - Yield Curve data from the Federal Reserve Economic Database (FRED) API. 

    '''

    def __init__(self,
                 method,
                 dates,
                 export_directories,
                 parent_directories=None,
                 saving_directory=None,
                 names_ticker_dictionary=None,
                 ):
        '''
        Example inputs:

        CBOE:
            - method: 'CBOE'
            - names: ['VIX_Futures']
            - parent_directories: ['"https://www.cboe.com/us/futures/market_statistics/historical_data/"]
            - names_ticker_dictionary = None

        YahooFinance:
            - method: 'YahooFinance'
            - names: ['SP500','VIX_Index','NAS100','DJIA30']
            - parent_directories: ['https://query1.finance.yahoo.com/v7/finance/download/']
            - names_ticker_dictionary:
                    {'SP500':'^GSPC', 'VIX_Index':'^VIX',
                        'NAS100': '^NDX', 'DJIA30': '^DJI'}
            - dates = ["2013-01-02", "2021-05-28"]


        FRED:
            - method = "FRED", 
            - names = None
            - parent_directories = None. (Note: the Fred package makes this link so you don't have to)
            - names_ticker_dictionary={'1m': "DGS1MO", '3m': "DGS3MO", '6m': "DGS6MO", 
                                        "1y": 'DGS1', "2y": 'DGS2',  "3y": "DGS3", 
                                        "5y": 'DGS5',  "7y": 'DGS7', "10y": 'DGS10',  
                                        "20y": 'DGS20', "30y": 'DGS30'}, 
            - dates = ["2013-01-02", "2021-05-28"]

        '''

        self.method = method
        self.dates = dates
        self.export_directories = export_directories

        if self.method[:4] =='CBOE':
            self.parent_directories = parent_directories
            self.saving_directory = saving_directory

        if names_ticker_dictionary is not None:
            self.names_ticker_dictionary = names_ticker_dictionary

    def implementation(self, export=False):
        '''
        Implement collection and save collected data.
        '''

        if self.method == 'YahooFinance':
            self.data = self._implement_YF()
            if export:
                self.data.to_csv(self.export_directories)

        elif self.method == "FRED":
            self.data = self._implement_FRED()
            if export:
                self.data.to_csv(self.export_directories)

        elif self.method == "CBOE_list_only":

            self._implement_CBOE_list()

            if export:

                df(self.vix_list).to_csv(self.saving_directory)

        elif self.method == "CBOE":

            self._implement_CBOE_list()

            self.contract_dict = self._implement_CBOE()

            if export:

                for identifier, dataframe in self.contract_dict.items():

                    dataframe.to_csv(self.saving_directory +
                                     identifier.replace('/', "") + '.csv')
        

    def _implement_YF(self):
        '''
        Scraping data from Yahoo Finance based on yahoofinance api
        '''

        # Storing the dataframes in a dictionary by ticker
        merged_dataframe = df()

        for name, ticker in self.names_ticker_dictionary.items():

            hist = yf.download(ticker,
                               start=self.dates[0], end=self.dates[1])['Adj Close']

            if len(merged_dataframe) > 0:
                if len(hist.index) != len(merged_dataframe.index):
                    raise Exception("Data Must Have Common Dates")

            merged_dataframe[name] = hist

        return merged_dataframe


    def _implement_CBOE_list(self):

        def __scrape_individual_links(self,CBOE_web_text,parent_directories):
            '''
        Decomposing a global web request made to a webpage containing numerous securities
        into individual links for each security
            '''

        # These lists will contain the individual csv names and identifiers
        # For the desired securities

            CBOE_vix_csv_list = []
            CBOE_vix_identifiers = []

            # Parsing the larger web request to smaller "chunks" for each security url
            for block in BeautifulSoup(CBOE_web_text).find_all('a'):
                block = str(block)
                CBOE_vix_csv_list.append(parent_directories[0] + block[22: 49])
                CBOE_vix_identifiers.append(block[111: 131])

            return CBOE_vix_csv_list, CBOE_vix_identifiers

        def __combine_and_create(self,identifier_list,csv_links,
            identifier_termination = 18,
            csv_links_start = -11):
            new_id_list = []
            for i in range(0, len(identifier_list)):
                if str(identifier_list[i][:7]) == "VX+VXT ":
                    identifier_list[i] = identifier_list[i][0:identifier_termination] + \
                         ' ' + csv_links[i][csv_links_start:-1]

                    new_id_list.append(identifier_list[i])
            return new_id_list



        # Create a global web request directing to the CBOE securities directory
        self.CBOE_web_request = requests.get(
            self.parent_directories[0] + "products/VX/")

        # Process the global web request to get links for each security
        csv_links, identifier_list = __scrape_individual_links(self,
            self.CBOE_web_request.text,
            self.parent_directories)

        # Combine each security identifier with the url to create valid download link
        
        self.vix_list = __combine_and_create(identifier_list,csv_links)

        self.csv_links  = csv_links

    def _implement_CBOE(self):
        
        '''
        Scraping csv files from CBOE using BeautifulSoup web scraping functionality

        A sample file can be retrived here:
        https://www.cboe.com/us/futures/market_statistics/historical_data/products/csv/VX/2013-04-17/
        '''

        # Create a dictionary with key, value pairs corresponding to identifiers and dataframes respectively

        CBOE_vix_csv_dict = {}

        for i in tqdm(range(len(self.vix_list)), position=0, leave=True):
            data=rc(self.csv_links[i]).set_index("Trade Date")
            CBOE_vix_csv_dict[self.vix_list[i]] = data

        return CBOE_vix_csv_dict

    

    def _implement_FRED(self):
        '''
        # Loading each item of treasury data using the FRED api.
        # Note: if you need a FRED api key, visit this link:
        # https://fredaccount.stlouisfed.org/apikey
        '''

        treasury_data = df()

        fred_api_key = input("Please enter your FRED database API key:  ")

        for name, treasury_ticker in self.names_ticker_dictionary.items():

            fred_access = Fred(fred_api_key)

            data = fred_access.get_series(treasury_ticker,
                                          observation_start=self.dates[0],
                                          observation_end=self.dates[1])

            treasury_data[name] = data

        return treasury_data


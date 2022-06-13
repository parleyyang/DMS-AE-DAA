import numpy as np
from pandas import DataFrame as df
from pandas import read_csv as rc
from sklearn.linear_model import LinearRegression as linreg


class DataProcessing:
    '''
    Description:

    Two categories of data we can process:
     - YahooFinance: Indices: SP500, VIX Index, Nasdaq100, DJIA30 indices from Yahoo Finance
     - FRED: Yield Curve data from Fred

    '''

    '''
    Example inputs:
        import_directory='../Dataset/fred_raw.csv', 
        export_directory='../Dataset/fred/'

    OR
        import_directory='../Dataset/yf_raw.csv', 
        export_directory='../Dataset/yf/'
    

    '''

    def __init__(self, 
        import_directory='../Dataset/fred_raw.csv', 
        export_directory='../Dataset/fred/'
        ):
        self.import_directory = import_directory
        self.export_directory = export_directory

    def __call__(self,
        data_type='FRED',
        horizonrange=range(1,11),
        export_mode=False):

        if data_type == 'YahooFinance':
            self.data = rc(self.import_directory,index_col='Date')
            self.processed_data = self._process_yf_data(horizonrange=horizonrange)

        elif data_type == "FRED":
            self.data = rc(self.import_directory,index_col='Unnamed: 0')
            self.processed_data = self.data.copy()
            self.processed_data['Full_Slope'] = self._process_fred_data()

        if export_mode:
            self.processed_data.to_csv(self.export_directory+data_type+'.csv')

    def _process_yf_data(self,horizonrange):
        ''' 
        Processes each stock market index
        By calculating returns across multiple horizons
        And saving to a dataframe
        '''

        self.data.dropna(inplace=True)

        return_df = df()
        for column in self.data:
            for horizon in horizonrange:

                # pg. 14 of the paper for return calculation
                return_df[f"{column}-{horizon}-return"] = np.log(
                    self.data[column]) - np.log(self.data[column].shift(horizon))

        return return_df

    def _process_fred_data(self):
        '''
        Processing raw yield curve data by generating Fassas slopes. 
        See https://arxiv.org/pdf/2110.11156 for details of slope calculation
        '''
        def __gen_rates_curve_slopes(self, maturity_data, days_to_maturity, max_vals, start=0):
            '''
            Generates the slope of the yield curve based on pre-determined maturity dates.
            See pg. 25 of https://arxiv.org/abs/2110.11156 for more details.
            '''

            slopes = []
            maturity_data = np.array(maturity_data)
            for i in range(len(maturity_data)):

                row = maturity_data[i]
                days_exp = days_to_maturity[i]
                open_contracts = [[x] for x in row[start:max_vals]]
                open_contracts_days = [[x] for x in days_exp[start:max_vals]]
                fit_mat = np.array([open_contracts, open_contracts_days])
                model = linreg(fit_intercept=True)
                reg = model.fit(fit_mat[1], fit_mat[0])
                slopes.append(float(reg.coef_))
            return slopes

        self.data.dropna(inplace=True)

        M, Y = 30, 365

        treasury_days_matrix = np.array(
            [[M, int(Y/4), int(Y/2), Y, Y*2, Y*3, Y*5, Y*7, Y*10, Y*20, Y*30]]*len(self.data))

        yc_slopes = __gen_rates_curve_slopes(self,
            self.data, treasury_days_matrix, treasury_days_matrix.shape[1])

        return df(yc_slopes, index=self.data.index,
                  columns=["Rates Curve Slope"])

    

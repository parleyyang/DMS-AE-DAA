import pandas as pd
import numpy as np
from pandas import DataFrame as df
from pandas import read_csv as rc
import glob
from sklearn.linear_model import LinearRegression as linreg



class VIXDataProcessing:

    '''
    Description:
        Data we need to process:
        - CBOE: VIX Futures from CBOE (dict).

        Outline of the steps involved in the processing (as is represented by the functions that follow):

        1. Creating the VIX Series from the VIX contracts

        ---------------------

        1.1. Extract the expiration date from each contract.

        1.2. Order the contract identifiers based on these expiration dates. This helps to create a continuous
        and ordered series of VIX contract prices.

        1.3. Concatenate the contracts to create the continuous and ordered series.
        ---------------------

        2. Generate the slope of the series (curve) at all time points in the data set

        ---------------------
        2.1. Extract the pseudo expiration date for each of the VIX contracts.

        2.2. Find the days to expiration based on the contract identifiers.

        2.3. Merge the Spot VIX data scraped from YahooFinance with the rest of the contracts.

        2.4. Drop uncommon dates between VIX Index and VIX futures contracts. In our case this corresponds to 
        2015-04-03 and 2018-12-05.

        2.5. Perform the slope calculation as is described in pg. 25 of https://arxiv.org/abs/2110.11156.
        ---------------------
    '''

    def __init__(self,
                 export_directory='../Dataset/VIX_processed/Processed.csv',
                 import_directory='../Dataset/VIX_contracts/*.csv',
                 spot_vix=rc("../Dataset/yf_raw.csv", index_col = "Date")['VIX_Index'],
                 days_of_liquidity=1,
                 contract_maturity=range(1,9),
                 export_mode=True,
                 dates_range=["2013-01-01", "2022-01-31"], 
                 dates_to_drop=['2015-04-03', '2018-12-05'], # Good Friday and George H. W. Bush Memorial Day
                 ):

        VIX_dataframes = {}

        for file in glob.glob(import_directory):
            name_of_contract = file.split("/")[-1][:-4]
            VIX_dataframes[name_of_contract] = rc(file).set_index("Trade Date")
            VIX_dataframes[name_of_contract].index = pd.to_datetime(
                VIX_dataframes[name_of_contract].index)

        self.data = VIX_dataframes
        self.export_directory = export_directory
        self.spot_vix = spot_vix
        self.date_str_pos = 10  # this picks up the position of dates from identifier
        self.export_mode = export_mode
        self.start_date = dates_range[0]
        self.end_date = dates_range[1]
        ## LOOK AT THE DATES SETTING
        self.days_of_liquidity = days_of_liquidity
        self.contract_maturity = contract_maturity
        self.max_contract_maturity = contract_maturity.stop -1

        if dates_to_drop != None:
            self.dates_to_drop = [pd.to_datetime(
                date) for date in dates_to_drop]

    def __call__(self):
        '''
        Function to implement processing of VIX data.

        Processes unordered VIX contracts (input dict) by ordering the contracts by expiration dates,
        before combining them into a large dataframe.
        '''

        # Step 1.1.
        unordered_contract_expiration_dates = self._extract_date_from_contract_identifier()

        # Step 1.2.
        self._order_contracts_based_on_dates(unordered_dates=unordered_contract_expiration_dates)


        # Example of an output: an array with identifiers, e.g. 'VX+VXT F1 (Jan 21)2021-01-20'
        # Step 1.3.
        self._process_maturity_data()
        

        # Step 1.4.
        dfs_to_concat = [self.concatenated_dict[f'vix_{i}m'] for i in self.contract_maturity]
        self.merged_df = pd.concat(dfs_to_concat, axis=1)

        # -------
        # Slope calculation
        # -------

        # Step 2.1.
        pseudo_date_list = self._extract_pseudo_exp_date_list_from_monthly_vix_dict()

        # Step 2.2.
        self._extract_days_to_expiry(pseudo_date_list)

        # Step 2.3.
        self.spot_vix.index = pd.DatetimeIndex(self.spot_vix.index)
        self.merged_df = pd.concat([self.spot_vix, self.merged_df], axis=1)[
            self.start_date:self.end_date]

        # Step 2.4.
        self.merged_df = self.merged_df.drop(self.dates_to_drop)

        # Step 2.5.
        self._gen_vix_curve_slopes()

        # Step 2.6.
        if self.export_mode:
            output=self.merged_df.join(self.vix_slope_df)
            output.to_csv(self.export_directory)


    def _extract_date_from_contract_identifier(self):
        '''
        Contracts come with unique identifiers adn they include expiration dates, which we can extract.
        '''
        dict_of_dataframes = self.data
        unordered_dates_from_identifier = []
        original_identifers = dict_of_dataframes.keys()
        for identifier in original_identifers:
            date_from_identifier = identifier[-self.date_str_pos:]
            unordered_dates_from_identifier.append(date_from_identifier)

        return unordered_dates_from_identifier

    def _order_contracts_based_on_dates(self, unordered_dates):
        '''
        Takes unordered dates and returns ordered contract identifiers.
        '''

        ordered_identifiers = []
        original_identifiers = self.data.keys()
        unordered_dates.sort()
        for unordered_date in unordered_dates:
            for original_identifier in original_identifiers:
                if original_identifier[-self.date_str_pos:] == unordered_date:
                    ordered_identifiers.append(original_identifier)
        
        self._ordered_contracts_based_on_dates = ordered_identifiers

    def _process_maturity_data(self):
        '''
        Concatenating contracts together based on time-to-expiration.
        E.g. One month contracts will be combined to make a long continuous series.
        Same applies to other maturities.
        '''
        ordered_identifiers = self._ordered_contracts_based_on_dates

        def __concatenate_contracts(self, contract_month=1):
            '''
            Concatenates the open contracts depending on maturity.
            '''

            def ___concatenate_front_month(self):
                '''
                Concatenates open front-month contracts
                '''
                

                concat_futures_ts_dataframe = self.data[ordered_identifiers[0]].iloc[:-self.days_of_liquidity]

                last_date_from_prev_contract = concat_futures_ts_dataframe.index[-1]

                for identifier in ordered_identifiers[1:]:
                    df = self.data[identifier].iloc[:-self.days_of_liquidity]
                    concat_futures_ts_dataframe = pd.concat(
                        [concat_futures_ts_dataframe, df[df.index > last_date_from_prev_contract]])

                    last_date_from_prev_contract = concat_futures_ts_dataframe.index[-1]

                return concat_futures_ts_dataframe

            def ___concatenate_other_months(self, contract_month):
                '''
                Concatenates open contracts with maturities other than front-month
                '''

                def ____make_range_for_start_of_iterations():
                    return range(contract_month, len(ordered_identifiers))

                front_month_expiry = self.data[ordered_identifiers[0]].index[-self.days_of_liquidity]
                # print(front_month_expiry)

                candidatedf = self.data[ordered_identifiers[contract_month-1]]

                concat_futures_ts_dataframe = candidatedf[candidatedf.index< front_month_expiry]

                last_date_from_prev_contract = concat_futures_ts_dataframe.index[-1]
                # print(last_date_from_prev_contract)

                irange = ____make_range_for_start_of_iterations()

                for i in irange:
                    key = ordered_identifiers[i]

                    front_month_expiry = self.data[ordered_identifiers[i -
                                        (contract_month-1)]].index[-self.days_of_liquidity]

                    candidatedf = self.data[key][self.data[key].index < front_month_expiry]
                    candidatedf = candidatedf[candidatedf.index > last_date_from_prev_contract]

                    concat_futures_ts_dataframe = pd.concat([concat_futures_ts_dataframe, candidatedf])
                    last_date_from_prev_contract = concat_futures_ts_dataframe.index[-1]
                    # print(last_date_from_prev_contract)

                return concat_futures_ts_dataframe

            if contract_month == 1:
                return ___concatenate_front_month(self)

            else:
                return ___concatenate_other_months(self, contract_month)


        concatenated_dict = {}

        for contract_month in self.contract_maturity:
            concatenated_futures_df = __concatenate_contracts(self, contract_month=contract_month)

            concatenated_futures_df = concatenated_futures_df[~concatenated_futures_df.index.duplicated(
            )]

            concatenated_dict[f'vix_{contract_month}m'] = concatenated_futures_df['Close'].rename(
                f"vix_{contract_month}m")

        self.concatenated_dict = concatenated_dict

    def _extract_pseudo_exp_date_list_from_monthly_vix_dict(self):
        '''
        Extracting the psuedo expiration date from the dictionary of monthly VIX contracts
        '''

        pseudo_exp_date_list = []
        pseudo_exp_date_dict = {}
        for key in self._ordered_contracts_based_on_dates:
            df = self.data[key]
            exp = str(df.index[-1])[:self.date_str_pos]
            pseudo_exp = self.merged_df.index[self.merged_df.index <=
                                              exp][-self.days_of_liquidity]
            if pseudo_exp not in pseudo_exp_date_list:
                pseudo_exp_date_list.append(pseudo_exp)
                pseudo_exp_date_dict.update({exp: pseudo_exp})
        return pseudo_exp_date_dict

    def _extract_days_to_expiry(self,pseudo_exp_date_dict):
        '''
        Extracting the number of days until the expiry of the vix contract depending on maturity.
        Note there are alternatives such as https://pandas.pydata.org/docs/reference/api/pandas.Series.asfreq.html
        '''
        max_contract_maturity = self.max_contract_maturity
        ordered_identifiers = self._ordered_contracts_based_on_dates
        dates_before_expiry_dict = {}
        days_to_expiry = {}

        # First for-loop finds which dates occur before expiry in our series.
        for i in range(len(ordered_identifiers)-(max_contract_maturity+1)):
            exp_date_key = ordered_identifiers[i][-self.date_str_pos:]
            pseudo_exp = pseudo_exp_date_dict[exp_date_key]
            vix_key = ordered_identifiers[i]
            dates_before_date_key = []
            if i == 0:
                dates_before_date_key = [
                    date for date in self.merged_df.index if pd.to_datetime(date) < pseudo_exp]
            else:
                last_date = pseudo_exp_date_dict[sorted(
                    pseudo_exp_date_dict.keys())[i-1]]
                dates_before_date_key = [
                    date for date in self.merged_df.index if pd.to_datetime(date) < pseudo_exp]
                dates_before_date_key = [
                    date for date in dates_before_date_key if pd.to_datetime(date) >= last_date]

            dates_before_expiry_dict.update(
                {vix_key: dates_before_date_key})

            # Second for-loop finds the number of days to maturity for each day.
            for date in dates_before_date_key:
                day_list = [0]  # This is for spot vix
                for j in range(max_contract_maturity):
                    next_vix_key = ordered_identifiers[i+j]
                    next_df = self.data[next_vix_key]
                    if date in next_df.index:
                        days = (pd.to_datetime(
                            next_vix_key[-self.date_str_pos:]) - next_df.loc[date].name).days
                        day_list.append(days)

                days_to_expiry.update({date: day_list})

        self.days_to_maturity = df(days_to_expiry).transpose()

    def _gen_vix_curve_slopes(self,vix_export_mode=False):
        '''
        Final Step:
        Processing raw vix maturity data to generate Fassas slopes. 
        See pg.25 of https://arxiv.org/pdf/2110.11156 for details of slope calculation.
        '''

        slopes = []
        dates = []

        maturity_array = np.array(self.merged_df)
        days_to_maturity = np.array(self.days_to_maturity)

        for i in range(len(self.merged_df)):
            # print(i)
            row = maturity_array[i]
            days_exp = days_to_maturity[i]
            open_contracts = [[x] for x in row if x != 0]
            open_contracts_days = [[x] for x in days_exp[:len(open_contracts)]]
            fit_mat = np.array([open_contracts, open_contracts_days])
            model = linreg(fit_intercept=True)
            reg = model.fit(fit_mat[1], fit_mat[0])
            slopes.append(float(reg.coef_))
            dates.append(self.merged_df.index[i])

        vix_slope_df = df([dates, slopes]).transpose()
        vix_slope_df.columns = ["Date", 'vix_slope']
        vix_slope_df.set_index("Date", inplace=True)
        if vix_export_mode:
            vix_slope_df.to_csv(self.export_directory)

        self.vix_slope_df = vix_slope_df 

from strategy import Strategy
from event import SignalEvent
import numpy as np
from math import floor, ceil
from statsmodels.tsa.vector_ar.vecm import coint_johansen
import pandas as pd 

SECONDS_IN_YEAR = 31536000
class StatArbMultiTrade(Strategy):
 
    def __init__(self, events, rates, lookback_window=10):
        self.rates = rates
        self.events = events
        self.token_list = self.rates.token_list
        self.lookback_window = lookback_window
        
        # Keep a record of the rates data and the state of the market
        self.latest_rates = -1.0*np.ones(len(self.token_list)) 
        self.invested = None
        self.days = 0
        
        self.deviations = 1 # Default is 1 standard deviation away from the mean spread
        
        # Need to keep track of the Kalman filter moving averages and moving stds
        self.days_to_lookback = -self.lookback_window
        self.spread_recent_L_values = {l: 0 for l in range(self.days_to_lookback,0)}
        self.spread_MA = 0 
        self.spread_MStd = 0
        
        self.pos = np.zeros(len(self.token_list))

    def liquidity_index_to_apy(self, rates):
        liq_idx = np.array([r[2] for r in rates])
        timestamps = np.array([r[1] for r in rates])
        apys = []
        for i in range(1, len(liq_idx)):
            window = i-self.lookback_window if self.lookback_window<=i else 0
            variable_rate = liq_idx[i]/liq_idx[window] - 1.0 
            # Annualise the rate
            compounding_periods = SECONDS_IN_YEAR / (timestamps[i] - timestamps[window]).total_seconds()
            apys.append(((1 + variable_rate)**compounding_periods) - 1)
        return apys
        
    def calculate_signals(self, event):
        # Pick up the latest rates here
        if event.type == "MARKET":
            liq_idxs ={
                token: self.rates.get_latest_rates(token, N=self.lookback_window) for token in self.token_list
            }
            # Put all relevant rates in the lookback window here
            df_rates = pd.DataFrame.from_dict({
                    k: self.liquidity_index_to_apy(rates=v) for k, v in liq_idxs.items()
                }
            )   
            self.latest_rates = df_rates.iloc[-1].values
            self.days += 1
            if all(self.latest_rates > -1.0):
                # 1) Johansen test to extract eigenvalues for positions
                jres = coint_johansen(df_rates, det_order=0, k_ar_diff=1) 
     
                # 2) Form spread from the critical values of the Johansen test
                leading_evecs = jres.evec[:,0] # Leading eigenvectors to form the stationary series
                spread = 0
                for i in range(len(leading_evecs)):
                    spread += self.latest_rates[i]*leading_evecs[i]
                
                # 3) Save the relevant historical spread values, according to the lookback window
                self.spread_recent_L_values[self.days_to_lookback] = spread
                non_zero_spread_cached = np.array([v for k,v in self.spread_recent_L_values.items() if v!=0])
                self.days_to_lookback += 1
                if self.days_to_lookback==0: # Reset in order to cache historical values within the lookback only
                    self.days_to_lookback = -self.lookback_window
            
                # 4) Update the moving average and moving std according to the latest values cached
                self.spread_MA = non_zero_spread_cached.mean()
                self.spread_MStd = non_zero_spread_cached.std()
                
                # Updated Z-score on the spread 
                spread_z = (spread - self.spread_MA)/self.spread_MStd
            
                # If there is not curenctly a position in the market
                # Compute the relevant posiyions to take, based on the leading eigenvector
                normed_evecs = leading_evecs/jres.evec[0][0] # Normalise the leading eigenvectors for positions
      
                if self.invested is None:
                    # Get position sizes (directions are considered separately)
                    self.pos = normed_evecs
                    idx_pos = [int(ceil(np.abs(i))) for i in self.pos]
                    if spread_z < -self.deviations: # Long the spread
                        for i in range(idx_pos[0]): # Always long the first term, by construction
                            signal = SignalEvent(list(liq_idxs.values())[0][-1][0], "LONG", list(liq_idxs.values())[0][-1][1])
                            self.events.put(signal)
                        for i in range(1, len(idx_pos)):
                            POS = "SHORT" if self.pos[i]<0 else "LONG"
                            for j in range(idx_pos[i]):
                                signal = SignalEvent(list(liq_idxs.values())[i][-1][0], POS, list(liq_idxs.values())[i][-1][1])
                                self.events.put(signal)                       
                        self.invested = "LONG"

                    elif spread_z > self.deviations: # Short the spread
                        for i in range(idx_pos[0]): 
                            signal = SignalEvent(list(liq_idxs.values())[0][-1][0], "SHORT", list(liq_idxs.values())[0][-1][1])
                            self.events.put(signal)
                        for i in range(1, len(idx_pos)):
                            POS = "LONG" if self.pos[i]<0 else "SHORT"
                            for j in range(idx_pos[i]):
                                signal = SignalEvent(list(liq_idxs.values())[i][-1][0], POS, list(liq_idxs.values())[i][-1][1])
                                self.events.put(signal)
                        self.invested = "SHORT"

                # If instead we have already entered a position in the market
                if self.invested is not None:
                    self.pos = normed_evecs
                    idx_pos = [int(ceil(np.abs(i))) for i in self.pos]
                    if self.invested=="LONG" and spread_z > 0: # Unwind the long spread position
                        for i in range(idx_pos[0]):
                            signal = SignalEvent(list(liq_idxs.values())[0][-1][0], "SHORT", list(liq_idxs.values())[0][-1][1])
                            self.events.put(signal)
                        for i in range(1, len(idx_pos)):
                            POS = "LONG" if self.pos[i]<0 else "SHORT"
                            for j in range(idx_pos[i]):
                                signal = SignalEvent(list(liq_idxs.values())[i][-1][0], POS, list(liq_idxs.values())[i][-1][1])
                                self.events.put(signal)
                        self.invested = None
                    
                    elif self.invested=="SHORT" and spread_z < 0: # Unwind the short spread position
                        for i in range(idx_pos[0]):
                            signal = SignalEvent(list(liq_idxs.values())[0][-1][0], "LONG", list(liq_idxs.values())[0][-1][1])
                            self.events.put(signal)
                        for i in range(1, len(idx_pos)):
                            POS = "SHORT" if self.pos[i]<0 else "LONG"
                            for j in range(idx_pos[i]):
                                signal = SignalEvent(list(liq_idxs.values())[i][-1][0], POS, list(liq_idxs.values())[i][-1][1])
                                self.events.put(signal)
                        self.invested = None
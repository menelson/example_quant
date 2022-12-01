from strategy import Strategy
from event import SignalEvent
import numpy as np
from math import floor, ceil
import pandas as pd

SECONDS_IN_YEAR = 31536000
class KalmanFilterStatArb(Strategy):
    """
    This is a generalised pairs trading strategy, where a Kalman Filter (state-space model with
    a Bayesian update) is used to update the hedge ratio dynamically, and based on the covariance
    of the hedge ratio measurement we can take positions where the spread between the rates of the
    pairs is long or short, with a tracker on when different positions should be unwound

    TODO: make the trading compatible with positions on Voltz
    """

    def __init__(self, events, rates, use_dynamic_hedge=False, lookback_window=10):
        self.rates = rates
        self.events = events
        self.token_list = self.rates.token_list
        self.lookback_window = lookback_window
        
        # Keep a record of the rates data and the state of the market
        self.latest_rates = -1.0*np.ones(len(self.token_list)) 
        self.invested = None
        self.days = 0

        # Kalman filter-specific parameters
        self.delta = 1e-4
        self.wt = self.delta/(1-self.delta) * np.eye(len(self.token_list)) # Starting system error
        self.vt = 1e-3 # Starting measurement error
        self.theta = np.zeros(len(self.token_list)) # Starting hidden state
        self.C = np.zeros((len(self.token_list),len(self.token_list)))
        self.R = None

        self.qty = 1 # Default to 1 base unit, but can be scaled up 
        self.hedge_qty = self.qty*np.ones(len(self.token_list))
        
        self.deviations = 1 # Default is 1 standard deviation away from the mean spread
        
        # Need to keep track of the Kalman filter moving averages and moving stds
        self.days_to_lookback = -self.lookback_window
        self.et_recent_L_values = {l: 0 for l in range(self.days_to_lookback,0)}
        self.et_MA = 0 
        self.et_MStd = 0
        
        self.use_dynamic_hedge = use_dynamic_hedge
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
                # Observation matrix and forecast rates, formed by treating the 
                # the rate indexed at 1 as the observation (the other rate is a hidden variable) 
                # Last rate is the observation => apply hedge ratio to the other rates
                #F = np.asarray([r for r in self.latest_rates if r!=self.latest_rates[-1] else 1.0])#.reshape((1,2))
                
                F = np.append(self.latest_rates[:-1], 1.0)#.reshape((1,len(self.token_list)))
                y = self.latest_rates[1] # --> this comes from the last token, which we treat as the observation

                # Assume the prior value of the hidden states, theta_t, is distributed
                # as a multivariate Gaussian with mean a_t and covariance R_t
                if self.R is not None:
                    self.R = self.C + self.wt
                else:
                    self.R = np.zeros((len(self.token_list),len(self.token_list)))
                
                # Kalman filter update 
                # 1) Prediction of new observation as well as forecast error of that prediction
                yhat = F.dot(self.theta)
                #print("yhat: ", yhat) 
                et = y - yhat
                    
                # 2) Q_t calculation: the variance of the prediction on the observations
                Qt = F.dot(self.R).dot(F.T) + self.vt
                sqrt_Qt = np.sqrt(Qt)

                # 3) Posterior value of the hidden states, assuming that the hidden state prior is 
                # distributed as a multivariate Gaussian with mean m_t and and covariance C_t
                At = self.R.dot(F.T)/Qt # Kalman gain
                self.theta = self.theta + At.flatten() * et # State update
                self.C = self.R - At * F.dot(self.R) # State covariance update
              
                # 4) Save the relevant historical et values, according to the lookback window
                historical_et = self.et_recent_L_values[self.days_to_lookback] if self.days > self.lookback_window else None
                self.et_recent_L_values[self.days_to_lookback] = et
                non_zero_et_cached = np.array([v for k,v in self.et_recent_L_values.items() if v!=0])
                self.days_to_lookback += 1
                if self.days_to_lookback==0: # Reset in order to cache historical values within the lookback only
                    self.days_to_lookback = -self.lookback_window
            
                # 5) Update the moving average and moving std according to the latest values cached
                self.et_MA = non_zero_et_cached.mean()
                self.et_MStd = non_zero_et_cached.std()
                #if self.days < 20:
                #    print("MA: ", self.et_MA, ", MVar: ", self.et_MStd)
                
                # Updated Z-score on the spread 
                et_z = (et - self.et_MA)/self.et_MStd    
        
                # Get the hedge ratios
                normed_hedge = self.theta/self.theta[-1]
                
                if self.days > self.lookback_window:
                    if self.use_dynamic_hedge:
                        if self.invested is None:
                            self.pos = normed_hedge # Position directions and sizes
                            self.hedge_qty = [int(floor(np.abs(i))) for i in self.pos] # Position sizes
                            if et_z < -self.deviations:
                                # Long entry => long the rate we use in the observation, short the hidden rate according 
                                # to the Kalman Filter hedge ratio
                                for i in range(self.hedge_qty[-1]): # Always long the last term, by construction
                                    signal = SignalEvent(list(liq_idxs.values())[-1][-1][0], "LONG", list(liq_idxs.values())[-1][-1][1])
                                    self.events.put(signal)
                                for i in range(len(self.hedge_qty)-1):
                                    POS = "SHORT" if self.pos[i]<0 else "LONG"
                                    for j in range(self.hedge_qty[i]):
                                        signal = SignalEvent(list(liq_idxs.values())[i][-1][0], POS, list(liq_idxs.values())[i][-1][1])
                                        self.events.put(signal)   
                                self.invested = "LONG"
                            
                            elif et_z > self.deviations:
                                # Short entry => short the rate we use in the observation, long the hidden rate according 
                                # to the Kalman Filter hedge ratio
                                for i in range(self.hedge_qty[-1]): 
                                    signal = SignalEvent(list(liq_idxs.values())[-1][-1][0], "SHORT", list(liq_idxs.values())[-1][-1][1])
                                    self.events.put(signal)
                                for i in range(len(self.hedge_qty)-1):
                                    POS = "LONG" if self.pos[i]<0 else "SHORT"
                                    for j in range(self.hedge_qty[i]):
                                        signal = SignalEvent(list(liq_idxs.values())[i][-1][0], POS, list(liq_idxs.values())[i][-1][1])
                                        self.events.put(signal)
                                self.invested = "SHORT"

                        # If instead we have already entered a position in the market
                        if self.invested is not None:
                            self.pos = normed_hedge 
                            self.hedge_qty = [int(floor(np.abs(i))) for i in self.pos]
                            if self.invested=="LONG" and et_z > 0: # Unwind the long spread position
                                for i in range(self.hedge_qty[-1]): 
                                    signal = SignalEvent(list(liq_idxs.values())[-1][-1][0], "SHORT", list(liq_idxs.values())[-1][-1][1])
                                    self.events.put(signal)
                                for i in range(len(self.hedge_qty)-1):
                                    POS = "LONG" if self.pos[i]<0 else "SHORT"
                                    for j in range(self.hedge_qty[i]):
                                        signal = SignalEvent(list(liq_idxs.values())[i][-1][0], POS, list(liq_idxs.values())[i][-1][1])
                                        self.events.put(signal)
                                self.invested = None
                            
                            elif self.invested=="SHORT" and et_z < 0: # Unwind the short spead position
                                for i in range(self.hedge_qty[-1]): # Always long the last term, by construction
                                    signal = SignalEvent(list(liq_idxs.values())[-1][-1][0], "LONG", list(liq_idxs.values())[-1][-1][1])
                                    self.events.put(signal)
                                for i in range(len(self.hedge_qty)-1):
                                    POS = "SHORT" if self.pos[i]<0 else "LONG"
                                    for j in range(self.hedge_qty[i]):
                                        signal = SignalEvent(list(liq_idxs.values())[i][-1][0], POS, list(liq_idxs.values())[i][-1][1])
                                        self.events.put(signal)   
                                self.invested = None          
                    else:
                        # If there is not curently a position in the market
                        if self.invested is None:
                            if et_z < -self.deviations:
                                signal = SignalEvent(list(liq_idxs.values())[-1][-1][0], "LONG", list(liq_idxs.values())[-1][-1][1])
                                self.events.put(signal)  
                                for i in range(len(self.token_list)-1):
                                    signal = SignalEvent(list(liq_idxs.values())[i][-1][0], "SHORT", list(liq_idxs.values())[i][-1][1])
                                    self.events.put(signal)    
                                self.invested = "LONG"
                            elif et_z > self.deviations:
                                signal = SignalEvent(list(liq_idxs.values())[-1][-1][0], "SHORT", list(liq_idxs.values())[-1][-1][1])
                                self.events.put(signal)  
                                for i in range(len(self.token_list)-1):
                                    signal = SignalEvent(list(liq_idxs.values())[i][-1][0], "LONG", list(liq_idxs.values())[i][-1][1])
                                    self.events.put(signal)    
                                self.invested = "SHORT"
                        # If instead we have already entered a position in the market
                        if self.invested is not None:
                            if self.invested=="LONG" and et_z > 0: # Unwind the long spread position  
                                signal = SignalEvent(list(liq_idxs.values())[-1][-1][0], "SHORT", list(liq_idxs.values())[-1][-1][1])
                                self.events.put(signal)  
                                for i in range(len(self.token_list)-1):
                                    signal = SignalEvent(list(liq_idxs.values())[i][-1][0], "LONG", list(liq_idxs.values())[i][-1][1])
                                    self.events.put(signal)  
                                self.invested = None
                            elif self.invested=="SHORT" and et_z < 0: # Unwind the short spead position
                                signal = SignalEvent(list(liq_idxs.values())[-1][-1][0], "LONG", list(liq_idxs.values())[-1][-1][1])
                                self.events.put(signal)  
                                for i in range(len(self.token_list)-1):
                                    signal = SignalEvent(list(liq_idxs.values())[i][-1][0], "SHORT", list(liq_idxs.values())[i][-1][1])
                                    self.events.put(signal) 
                                self.invested = None
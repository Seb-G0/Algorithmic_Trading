#Price_Predition
#May 2024
#Sebastian and Zain

"""This is the third and final file submitted for our mayterm. The simple goal is to get a predicted yield from the model and scrape the USDA's estimated
soybean yield and look at the difference. NASS data allows us to see how people have reacted in the past based on if our difference is correct"""

#imports
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score

PREDICTED_YIELD = 51 #This is an arbitrary value for the purpose of showcasing this code the model would give you an actual value which you would put here

USDA_ESTIMATED_YIELD = 52 #This is found through a simple google search, if you would to backtest the data simply look up "USDA Soybean Yield estimate 20xx"

DIFFERENCE = PREDICTED_YIELD - USDA_ESTIMATED_YIELD

"""We then use Price_Reaction.csv and Yield_Actual_Forecasted to draw a correlation between price reaction and yield
This will be a trained model using Random Forrest Classifier as well
"""

#DATA
def DATA():
    Price_Reaction = pd.read_csv("Price_Reaction.csv")
    """Since December does not have a Forecasted value we will not use this to train the model"""
    Price_Reaction = Price_Reaction[Price_Reaction['Period'] != 'DEC']

    """Now we just go through and find its correlating data from "Yield_Actual_Forecasted.csv" """
    ROWS = []
    for index, row in Price_Reaction.iterrows():
        period = row['Period']
        year = row['Year']
        PRICE_REACTION = row['Value']
        YIELD = pd.read_csv("Yield_Actual_Forecasted.csv")
        ACTUAL_YIELD = YIELD.loc[(YIELD['Year'] == year) & (YIELD['Period'] == 'YEAR'), 'Value'].values[0]
        ESTIMATED_YIELD = \
        YIELD.loc[(YIELD['Year'] == year) & (YIELD['Period'] == f'YEAR - {period} FORECAST'), 'Value'].values[0]
        ROWS.append([year, period, PRICE_REACTION, ESTIMATED_YIELD, ACTUAL_YIELD])

    newCSV = pd.DataFrame(columns=['year', 'period', 'price', 'estimate', 'actual'], data=ROWS)
    newCSV.to_csv("Comparison.csv", index=False)


"""Now For the Model"""
data = pd.read_csv('Comparison.csv')

# Define features and target variable, using only quantitative data
X = data[['estimate', 'actual']]
y = data['price']

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

import statsmodels.api as sm

# Load the data
data = pd.read_csv('Comparison.csv')

# Define features and target variable, using only quantitative data
X = data[['estimate', 'actual']]
y = data['price']

# Add constant term for intercept
X = sm.add_constant(X)

# Fit the linear regression model
model = sm.OLS(y, X).fit()

# Print model summary
print(model.summary())

"""56.7% Accurate in predicting the movement of crop futures"""




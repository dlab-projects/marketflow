import pandas
import matplotlib.pyplot as plt
import numpy as np
data_df = pandas.read_csv('data.csv')
x = data_df['notes'].values
y = data_df['time'].values
print(x, y)
plt.plot(x,y, linewidth = 2.0)
plt.show()
import matplotlib.pyplot as plt
import numpy as np
x = np.random.randn(100)
y = 2 * x + np.random.randn(100) * 0.5
plt.figure(figsize=(6, 4))
plt.scatter(x, y, alpha=0.7)
plt.xlabel("X values")
plt.ylabel("Y values") 
plt.title("Sample Scatter Plot")
plt.grid(True, alpha=0.3)
plt.show()

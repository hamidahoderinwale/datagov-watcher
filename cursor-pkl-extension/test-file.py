#!/usr/bin/env python3
"""
Test file for Cursor PKL Extension
This file can be used to test the "Return to Context" functionality
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def analyze_data():
    """Analyze sample data and create visualizations"""
    # Create sample data
    data = pd.DataFrame({
        'x': np.random.randn(100),
        'y': np.random.randn(100),
        'value': np.random.randn(100)
    })
    
    # Basic statistics
    print("Dataset shape:", data.shape)
    print("Data types:")
    print(data.dtypes)
    print("\nBasic statistics:")
    print(data.describe())
    
    # Create visualization
    plt.figure(figsize=(12, 8))
    
    # Scatter plot
    plt.subplot(2, 2, 1)
    plt.scatter(data['x'], data['y'], c=data['value'], cmap='viridis', alpha=0.7)
    plt.xlabel('X values')
    plt.ylabel('Y values')
    plt.title('Scatter Plot: X vs Y')
    plt.colorbar(label='Value')
    
    # Histogram
    plt.subplot(2, 2, 2)
    plt.hist(data['x'], bins=20, alpha=0.7, color='blue')
    plt.xlabel('X values')
    plt.ylabel('Frequency')
    plt.title('Distribution of X values')
    
    # Time series (simulated)
    plt.subplot(2, 2, 3)
    time_series = np.cumsum(np.random.randn(100))
    plt.plot(time_series)
    plt.xlabel('Time')
    plt.ylabel('Value')
    plt.title('Time Series')
    
    # Correlation heatmap
    plt.subplot(2, 2, 4)
    correlation_matrix = data.corr()
    plt.imshow(correlation_matrix, cmap='coolwarm', aspect='auto')
    plt.colorbar()
    plt.title('Correlation Matrix')
    
    plt.tight_layout()
    plt.show()
    
    return data

if __name__ == "__main__":
    print("Starting data analysis...")
    result = analyze_data()
    print("Analysis complete!")



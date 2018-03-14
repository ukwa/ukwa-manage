import sys
import pandas as pd

"""
Reads a CSV file in and generates a sample, outputs to another file.
"""

df = pd.read_csv(sys.argv[1])
df.sample(n=sys.argv[2]).to_csv(sys.argv[3])


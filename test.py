import time
from collections import defaultdict
import nltk
from nltk import sentiment
from nltk.sentiment import SentimentIntensityAnalyzer
nltk.download("vader_lexicon")
sia = SentimentIntensityAnalyzer()
exit()

l = [str(i) for i in range(10000000)]
d = {str(i): True for i in range(10000000)}

st = time.monotonic_ns()
if '9999999' in l:
  print(True)
print(f'list lookup took {time.monotonic_ns() - st}')

st = time.monotonic_ns()
print(d['9999999'])
print(f'dict lookup took {time.monotonic_ns() - st}')

dd = defaultdict(bool)
for i in range(10000000):
  dd[i] = True

st = time.monotonic_ns()
try:
  dd['-1']
except:
  pass

print(f'defaultdict lookup took {time.monotonic_ns() - st}')

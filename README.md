# japan_address
for python lookup data

```python
import pickle
lu = pickle.load(open("lu_address.pkl", "rb"))
lu["北海道"] # -> ["01"]
lu["01"] # -> ["北海道"]
```

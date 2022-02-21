#!/usr/bin/env python3

import fuelwatch.fuelwatch as fw

opts3 = {'Product': 1, 'Suburb': "Queens Park"}
url3 = fw.generate_url(opts3)
data3 = fw.getdata(url3)
results3 = fw.parse(data3)

print("url3: ", url3)

print(results3)
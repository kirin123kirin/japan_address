#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import os
import pickle
from math import cos, sin, sqrt, atan2

"""
座標から最寄り駅探索
station_data.pklの出典:「国土数値情報(鉄道データ)」(国土交通省) (https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-N02-v3_0.html) (データ年度:令和3年度取得)
を加工編集
"""
filename = os.path.join(os.path.dirname(__file__), "dat/station_data.pkl")
with open(filename, "rb") as f:
    stations = pickle.load(f)

N = len(stations)


def get_station(lat, lon):
    scale = 2 ** 10
    ret = {}

    for dic in stations:
        dlat = dic["lat"] - lat
        dlon = dic["lon"] - lon

        if dlat ** 2 > 1 or dlon ** 2 > 1:
            continue

        a = sin(dlat / 2) ** 2 + cos(dic["lat"]) * cos(lat) * sin(dlon / 2) ** 2
        d = 12742.02 * atan2(sqrt(a), sqrt(1 - a))

        if d < scale:
            scale = d
            ret = dic
    return ret


if __name__ == "__main__":
    for x in sys.argv[1:]:
        print(get_station(*x.split(",")))

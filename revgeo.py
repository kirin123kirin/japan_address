#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pickle
import os
import re
from math import cos, sin, sqrt, atan2
import json
import sys

datadir = os.path.join(os.path.dirname(__file__), "dat")
maxint = sys.maxsize


def builddata(f=datadir + "/nlftp.mlit.go.jp/downloads/01_行政区域データ/町字レベル/r2ka*.geojson"):
    """
    e-Stat 統計地理情報システム 境界データダウンロードより取得したデータに基づいて作成
    https://www.e-stat.go.jp/gis/statmap-search?page=1&type=2&aggregateUnitForBoundary=A&toukeiCode=00200521&toukeiYear=2020&serveyId=A002005212020&datum=2011
    この作品は、クリエイティブ・コモンズの 表示 4.0 国際 ライセンスで提供されています。ライセンスの写しをご覧になるには、 http://creativecommons.org/licenses/by/4.0/ をご覧頂くか、Creative Commons, PO Box 1866, Mountain View, CA 94042, USA までお手紙をお送りください。 
    """

    from glob import glob
    from operator import itemgetter
    import pandas as pd

    extract = ["KEY_CODE", "PREF", "CITY", "PREF_NAME", "CITY_NAME", "S_NAME", "JINKO", "SETAI", "X_CODE", "Y_CODE"]
    ig = itemgetter(*extract)
    re_o = re.compile(r"[\[ \]]")

    def load_machi(fname):
        with open(fname, encoding="utf-8") as fp:
            # 元データshapeファイルからgeojsonへの変換はmapshaper(https://github.com/mbloch/mapshaper)を使用
            g = json.load(fp)
            for feat in g["features"]:
                coord = feat["geometry"]["coordinates"]
                coord = eval("[" + re_o.sub("", str(coord)) + "]")
                c1 = coord[::2]
                c2 = coord[1::2]
                yield dict(
                    **dict(zip(extract, ig(feat["properties"]))),
                    max_x=max(c1),
                    min_x=min(c1),
                    max_y=max(c2),
                    min_y=min(c2)
                )

    def summary(df, by, callback=None):
        for idx, gdf in df.groupby(by=by):
            cr = pd.concat([gdf.max()[["max_x", "max_y"]], gdf.min()[["min_x", "min_y"]]])
            yield cr.tolist(), callback(gdf) if callback else gdf

    areas = [[minmax, list(summary(gdf, "CITY", lambda x: x.iloc[:, :-4].to_dict(orient="records")))]
             for g in glob(f) for minmax, gdf in summary(pd.DataFrame(load_machi(g)), by="PREF")]

    filename = datadir + "/revgeo.pkl"
    with open(filename, "wb") as w:
        pickle.dump(areas, w)


def get_address(lat, lon):
    def contains(minmax):
        max_x, max_y, min_x, min_y = minmax
        if max_x < lat:
            return False
        if min_x > lat:
            return False
        if max_y < lon:
            return False
        if min_y > lon:
            return False
        return True

    def distance(dic):
        dlat = dic["X_CODE"] - lat
        dlon = dic["Y_CODE"] - lon

        if dlat ** 2 > 1 or dlon ** 2 > 1:
            return maxint

        a = sin(dlat / 2) ** 2 + cos(dic["X_CODE"]) * cos(lat) * sin(dlon / 2) ** 2
        return 12742.02 * atan2(sqrt(a), sqrt(1 - a))

    dist = maxint
    ret = {}

    for i in [i for i, (minmax, _) in enumerate(areas) if contains(minmax)]:
        for j in [j for j, (minmax, _) in enumerate(areas[i][1]) if contains(minmax)]:
            for dic in areas[i][1][j][1]:
                d = distance(dic)
                if d < dist:
                    dist = d
                    ret = dic

    return ret


if __name__ == "__main__":
    # builddata()

    areas = pickle.load(open(datadir + "/revgeo.pkl", "rb"))

    for x in sys.argv[1:]:
        print(get_address(*x.split(",")))

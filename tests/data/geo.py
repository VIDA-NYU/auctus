import numpy
import numpy.random


SIZE = 50


def main():
    lat1, long1 = 40.7298648, -73.9986808
    lat1m, long1m = 40.73287, -74.002031
    lat2, long2 = 40.692725, -73.9865644
    lat2m, long2m = 40.694316, -73.988495

    latitudes = numpy.concatenate([
        numpy.random.normal(lat1, abs(lat1 - lat1m), SIZE),
        numpy.random.normal(lat2, abs(lat2 - lat2m), SIZE),
    ])
    longitudes = numpy.concatenate([
        numpy.random.normal(long1, abs(long1 - long1m), SIZE),
        numpy.random.normal(long2, abs(long2 - long2m), SIZE),
    ])

    print("id,lat,long")
    for i, (lat, long) in enumerate(zip(latitudes, longitudes)):
        print("place%02d,%f,%f" % (i, lat, long))


if __name__ == '__main__':
    main()

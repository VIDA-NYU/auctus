import numpy
import numpy.random
import os
import string


SIZE = 50


def main():
    lat1, long1 = 40.7298648, -73.9986808
    lat1m, long1m = 40.73287, -74.002031
    lat2, long2 = 40.692725, -73.9865644
    lat2m, long2m = 40.694316, -73.988495

    random = numpy.random.RandomState(1)
    latitudes = numpy.concatenate([
        random.normal(lat1, abs(lat1 - lat1m), SIZE),
        random.normal(lat2, abs(lat2 - lat2m), SIZE),
    ])
    random = numpy.random.RandomState(2)
    longitudes = numpy.concatenate([
        random.normal(long1, abs(long1 - long1m), SIZE),
        random.normal(long2, abs(long2 - long2m), SIZE),
    ])
    random = numpy.random.RandomState(3)
    heights = random.normal(50.0, 20.0, 2 * SIZE)

    data_dir = os.path.dirname(__file__)
    with open(os.path.join(data_dir, 'geo.csv'), 'w') as f_data:
        print("id,lat,long,height", file=f_data)
        for i, (lat, long, h) in enumerate(zip(latitudes, longitudes, heights)):
            if i == 42:
                i = ''
            else:
                i = 'place%02d' % i
            print("%s,%f,%f,%f" % (i, lat, long, h), file=f_data)

    with open(os.path.join(data_dir, 'geo_wkt.csv'), 'w') as f_data:
        print("id,coords,height", file=f_data)
        for i, (lat, long, h) in enumerate(zip(latitudes, longitudes, heights)):
            if i == 42:
                i = ''
            else:
                i = 'place%02d' % i
            print("%s,POINT (%f %f),%f" % (i, long, lat, h), file=f_data)

    random = numpy.random.RandomState(5)
    aug_latitudes = random.normal(lat1, abs(lat1 - lat1m), 10)
    aug_longitudes = random.normal(long1, abs(long1 - long1m), 10)

    with open(os.path.join(data_dir, 'geo_aug.csv'), 'w') as f_data:
        print("lat,long,id,letter", file=f_data)
        for i, (lat, long, letter) in enumerate(
            zip(aug_latitudes, aug_longitudes, string.ascii_letters),
            100,
        ):
            print("%f,%f,place%d,%s" % (lat, long, i, letter), file=f_data)


if __name__ == '__main__':
    main()

import csv
import logging
import os
import re


__version__ = '0.1'


logger = logging.getLogger(__name__)


class GeoData(object):
    def __init__(self, data_path):
        self._data_path = os.path.abspath(data_path)
        self._areas = {}
        self._area_names = {}
        self._areas_bounds = {}
        self._levels_loaded = set()

    @staticmethod
    def get_local_cache_path():
        if 'DATAMART_GEO_DATA' in os.environ:
            return os.path.expanduser(os.environ['DATAMART_GEO_DATA'])
        elif 'XDG_CACHE_HOME' in os.environ:
            cache = os.environ['XDG_CACHE_HOME']
        else:
            cache = os.path.expanduser('~/.cache')
        return os.path.join(cache, 'datamart-geo')

    @classmethod
    def from_local_cache(cls):
        return cls(cls.get_local_cache_path())

    @classmethod
    def download(cls, url, levels, dest=None):
        if dest is None:
            dest = cls.get_local_cache_path()

        # TODO: Download files to the local path, unless they haven't changed
        raise NotImplementedError

        return cls(dest)

    _re_area_filename = re.compile(r'^areas([0-9]+)\.csv$')

    def get_levels_available(self):
        levels = []
        for name in os.listdir(self._data_path):
            m = self._re_area_filename.match(name)
            if m is not None:
                levels.append(int(m.group(1), 10))
        return levels

    def load_area(self, level):
        # Load admin area information
        filename = 'areas%d.csv' % level
        with open(os.path.join(self._data_path, filename)) as fp:
            reader = iter(csv.reader(fp))
            try:
                row = next(reader)
            except StopIteration:
                raise ValueError("No rows in %s" % filename)
            if row != ['parent', 'admin', 'admin level', 'admin name']:
                raise ValueError("Invalid %s" % filename)
            count = 0
            for row in reader:
                parent, admin, level, name = row
                parent = parent or None
                level = int(level, 10)
                obj = Area(admin, name, level, parent)
                self._areas[admin] = obj
                self._area_names[name.lower()] = obj
                count += 1
        logger.info("Loaded %s, %d areas", filename, count)

        # Load admin area bounds
        filename = 'bounds%d.csv' % level
        if os.path.exists(os.path.join(self._data_path, filename)):
            with open(os.path.join(self._data_path, filename)) as fp:
                reader = iter(csv.reader(fp))
                try:
                    row = next(reader)
                except StopIteration:
                    raise ValueError("No rows in %s" % filename)
                if row != [
                    'admin',
                    'min long', 'max long', 'min lat', 'max lat',
                ]:
                    raise ValueError("Invalid %s" % filename)
                count = 0
                for row in reader:
                    admin = row[0]
                    bounds = [float(e) for e in row[1:]]
                    self._areas_bounds[admin] = bounds
                    count += 1
            logger.info("Loaded %s, %d bounding boxes", filename, count)

        self._levels_loaded.add(level)

    def resolve_names(self, names):
        results = []
        for name in names:
            name = name.strip().lower()
            if name in self._area_names:
                area = self._area_names[name]
                results.append(area)
            else:
                results.append(None)

        return results

    def get_bounds(self, area):
        return self._areas_bounds.get(area)


class Area(object):
    def __init__(self, area, name, level, parent):
        self.area = area
        self.name = name
        self.level = level
        self.parent = parent

    def __repr__(self):
        return '<%s "%s" (%s) level=%d parent=%s>' % (
            self.__class__.__module__ + '.' + self.__class__.__name__,
            self.name,
            self.area,
            self.level,
            self.parent,
        )

import csv
import math
import random


def reservoir_sampling(iterator, size, random_state):
    """Return the header (first element) + `size` elements picked randomly.

    Uses algorithm L.
    """
    out = []
    try:
        for _ in range(size + 1):
            out.append(next(iterator))
    except StopIteration:
        return out

    w = math.exp(math.log(random_state.random()))
    pick = pos = size
    while True:
        pick += math.floor(
            math.log(random_state.random()) / math.log(1 - w)
        )
        try:
            row = next(iterator)
            while pos < pick:
                row = next(iterator)
        except StopIteration:
            return out
        out[random_state.randint(1, size)] = row
        w *= math.exp(math.log(random_state.random()) / size)


def sample(src_filename, dst_filename, size=50):
    random_state = random.Random(0)
    with open(src_filename) as src:
        out = reservoir_sampling(iter(csv.reader(src)), size, random_state)

    with open(dst_filename, 'w', encoding='utf-8', newline='') as dst:
        writer = csv.writer(dst)
        for row in out:
            writer.writerow(row)

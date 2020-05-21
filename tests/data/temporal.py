from datetime import datetime, timedelta
import os
import random


def main():
    data_dir = os.path.dirname(__file__)

    with open(os.path.join(data_dir, 'daily.csv'), 'w') as f_daily:
        print('aug_date,rain', file=f_daily)
        date = datetime(2019, 4, 23)
        rand = random.Random(1)
        for _ in range(30):
            time = date.date().strftime('%Y%m%d')
            boolean = ['no', 'yes'][rand.randint(0, 1)]
            print('%s,%s' % (time, boolean), file=f_daily)
            date += timedelta(days=1)

    with open(os.path.join(data_dir, 'hourly.csv'), 'w') as f_hourly:
        print('aug_date,rain', file=f_hourly)
        date = datetime(2019, 6, 12)
        rand = random.Random(2)
        for _ in range(52):
            time = date.isoformat()
            boolean = ['no', 'yes'][rand.randint(0, 1)]
            print('%s,%s' % (time, boolean), file=f_hourly)
            date += timedelta(hours=1)


if __name__ == '__main__':
    main()

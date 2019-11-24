# WHERE clause is evaluated for every tuple therefore this updates roughly 15% of the tuples in the border table.

UPDATE border SET PortName='', PortCode=-1*(RAND()*(9999-100)+100), Date='11/21/1919 12:00:00 AM', Measurement=9999999 WHERE RAND() <= 0.15;
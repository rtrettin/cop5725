CREATE TABLE IF NOT EXISTS `border` (
  `id` int(11) NOT NULL,
  `PortName` varchar(100) NOT NULL,
  `State` varchar(100) NOT NULL,
  `PortCode` int(11) NOT NULL,
  `Border` varchar(100) NOT NULL,
  `Date` varchar(100) NOT NULL,
  `Measure` varchar(100) NOT NULL,
  `Measurement` int(11) NOT NULL,
  `Location` varchar(200) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
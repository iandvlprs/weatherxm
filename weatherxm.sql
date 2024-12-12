DROP TABLE IF EXISTS `station`;
CREATE TABLE `station` (
  `stdtt` datetime NOT NULL,
  `sttmp` float NOT NULL,
  `sthum` float NOT NULL,
  `stwsn` float NOT NULL,
  `stwsg` float NOT NULL,
  `stwdr` float NOT NULL,
  `stsol` float NOT NULL,
  `stuvi` float NOT NULL,
  `strmn` float NOT NULL,
  `strma` float NOT NULL,
  `stprs` float NOT NULL,
  `stdew` float NOT NULL,
  `strlf` float NOT NULL,
  `still` float NOT NULL,
  `stico` varchar(255) NOT NULL,
  PRIMARY KEY (`stdtt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

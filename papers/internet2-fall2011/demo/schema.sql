CREATE TABLE IF NOT EXISTS `nodes` (
  `id` int(11) NOT NULL auto_increment,
  `latitude` double NOT NULL,
  `longitude` double NOT NULL,
  `site` varchar(255) NOT NULL,
  `version` int(11) NOT NULL,
  PRIMARY KEY  (`id`)
) ENGINE=MyISAM ;


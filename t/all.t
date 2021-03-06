#!/usr/bin/perl -w

use strict;

use Data::Dumper;
sub ::p(@){ print Dumper(\@_);exit };

use Test::More;
use MySQL::Diff;
use MySQL::Diff::Database;

my $TEST_USER = 'test';
my @VALID_ENGINES = qw(MyISAM InnoDB);
my $VALID_ENGINES = join '|', @VALID_ENGINES;

my %tables = (
  foo1 => '
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  foreign_id INT(11) NOT NULL, 
  PRIMARY KEY (id)
);
',

  foo2 => '
# here be a comment

CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  foreign_id INT(11) NOT NULL, # another random comment
  field BLOB,
  PRIMARY KEY (id)
);
',

  foo3 => '
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  foreign_id INT(11) NOT NULL, 
  field TINYBLOB,
  PRIMARY KEY (id)
);
',

  foo4 => '
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  foreign_id INT(11) NOT NULL, 
  field TINYBLOB,
  PRIMARY KEY (id, foreign_id)
);
',

  foo5 => '
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  `foreign id` INT(11) NOT NULL, 
  PRIMARY KEY (id)
);
',

  foo6 => '
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  foreign_id INT(11) NOT NULL,
  PRIMARY KEY (id)
) PARTITION BY HASH (id);
',

  bar1 => '
CREATE TABLE bar (
  id     INT AUTO_INCREMENT NOT NULL PRIMARY KEY, 
  ctime  DATETIME,
  utime  DATETIME,
  name   CHAR(16), 
  age    INT
);
',

  bar2 => '
CREATE TABLE bar (
  id     INT AUTO_INCREMENT NOT NULL PRIMARY KEY, 
  ctime  DATETIME,
  utime  DATETIME,   # FOO!
  name   CHAR(16), 
  age    INT,
  UNIQUE (name, age)
);
',

  bar3 => '
CREATE TABLE bar (
  id     INT AUTO_INCREMENT NOT NULL PRIMARY KEY, 
  ctime  DATETIME,
  utime  DATETIME,
  name   CHAR(16), 
  age    INT,
  UNIQUE (id, name, age)
);
',

  baz1 => '
CREATE TABLE baz (
  firstname CHAR(16),
  surname   CHAR(16)
);
',

  baz2 => '
CREATE TABLE baz (
  firstname CHAR(16),
  surname   CHAR(16),
  UNIQUE (firstname, surname)
);
',

  baz3 => '
CREATE TABLE baz (
  firstname CHAR(16),
  surname   CHAR(16),
  KEY (firstname, surname)
);
',

  baz4 => '
CREATE TABLE baz (
  firstname CHAR(16),
  surname   CHAR(16),
  KEY `users name` (firstname, surname)
);
',

  qux1 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id)
);
},

  qux2 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id)
) PARTITION BY HASH (id);
},

  qux3 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id)
) PARTITION BY LINEAR HASH (id);
},

  qux4 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id)
) PARTITION BY KEY (id);
},

  qux5 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id, create_at)
) PARTITION BY RANGE (TO_DAYS(create_at)) (
  PARTITION p20130314 VALUES LESS THAN (735306),
  PARTITION p20130328 VALUES LESS THAN (735320)
);
},

  qux6 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id, create_at)
) PARTITION BY LIST (MONTH(create_at)) (
  PARTITION odd VALUES IN (1,3,5,7,9,11),
  PARTITION even VALUES IN (2,4,6,8,10,12)
);
},

  qux7 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id, create_at)
) PARTITION BY RANGE (TO_DAYS(create_at)) (
  PARTITION p20130314 VALUES LESS THAN (735306),
  PARTITION p20130328 VALUES LESS THAN (735320),
  PARTITION p20130329 VALUES LESS THAN (735321)
);
},

  qux8 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id, create_at)
) PARTITION BY RANGE (TO_DAYS(create_at))
SUBPARTITION BY HASH (`id`)
SUBPARTITIONS 2
(
  PARTITION p20130314 VALUES LESS THAN (735306),
  PARTITION p20130328 VALUES LESS THAN (735320),
  PARTITION p20130329 VALUES LESS THAN (735321)
)
},

  qux9 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id, create_at)
) PARTITION BY RANGE (TO_DAYS(create_at)) (
  PARTITION p20130314 VALUES LESS THAN (735306),
  PARTITION p20130328 VALUES LESS THAN (735320),
  PARTITION pmax VALUES LESS THAN MAXVALUE
);
},

  qux10 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id, create_at)
) PARTITION BY RANGE (TO_DAYS(create_at)) (
  PARTITION p20130314 VALUES LESS THAN (735306),
  PARTITION p20130328 VALUES LESS THAN (735320),
  PARTITION p20130329 VALUES LESS THAN (735321),
  PARTITION pmax VALUES LESS THAN MAXVALUE
);
},

  qux11 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id)
) PARTITION BY HASH (id) PARTITIONS 12;
},

  qux12 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id)
) PARTITION BY HASH (id) PARTITIONS 8;
},

  qux13 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id)
) PARTITION BY LINEAR HASH (id) PARTITIONS 12;
},

  qux14 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id)
) PARTITION BY LINEAR HASH (id) PARTITIONS 8;
},

  qux15 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id)
) PARTITION BY KEY (id) PARTITIONS 12;
},

  qux16 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id)
) PARTITION BY KEY (id) PARTITIONS 8;
},

  qux17 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id)
) PARTITION BY LINEAR KEY (id);
},

  qux18 => q{
CREATE TABLE foo (
  id INT(11) NOT NULL auto_increment,
  create_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (id, create_at)
) PARTITION BY RANGE (TO_DAYS(create_at))
SUBPARTITION BY HASH (`id`)
(
  PARTITION p20130314 VALUES LESS THAN (735306) (
    SUBPARTITION s0,
    SUBPARTITION s1
  ),
  PARTITION p20130328 VALUES LESS THAN (735320) (
    SUBPARTITION s2,
    SUBPARTITION s3
  ),
  PARTITION p20130329 VALUES LESS THAN (735321) (
    SUBPARTITION s4,
    SUBPARTITION s5
  )
)
},
);

my %tests = (
  'add column' =>
  [
    {},
    @tables{qw/foo1 foo2/},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` ADD COLUMN `field` blob;
',
  ],

  'add column 2' =>
  [
    {},
    @tables{qw/foo1 foo5/},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` DROP COLUMN `foreign_id`; # was int(11) NOT NULL
ALTER TABLE `foo` ADD COLUMN `foreign id` int(11) NOT NULL;
',
  ],
  
  'drop column' =>
  [
    {},
    @tables{qw/foo2 foo1/},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` DROP COLUMN `field`; # was blob
',
  ],

  'change column' =>
  [
    {},
    @tables{qw/foo2 foo3/},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` CHANGE COLUMN `field` `field` tinyblob; # was blob
'
  ],

  'no-old-defs' =>
  [
    { 'no-old-defs' => 1 },
    @tables{qw/foo2 foo1/},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
## Options: no-old-defs
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` DROP COLUMN `field`;
',
  ],

  'add table' =>
  [
    { },
    $tables{foo1}, $tables{foo2} . $tables{bar1},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` ADD COLUMN `field` blob;
CREATE TABLE `bar` (
  `id` int(11) NOT NULL auto_increment,
  `ctime` datetime default NULL,
  `utime` datetime default NULL,
  `name` char(16) default NULL,
  `age` int(11) default NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

',
  ],

  'drop table' =>
  [
    { },
    $tables{foo1} . $tables{bar1}, $tables{foo2},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

DROP TABLE `bar`;

ALTER TABLE `foo` ADD COLUMN `field` blob;
',
  ],

  'only-both' =>
  [
    { 'only-both' => 1 },
    $tables{foo1} . $tables{bar1}, $tables{foo2},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
## Options: only-both
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` ADD COLUMN `field` blob;
',
  ],

  'keep-old-tables' =>
  [
    { 'keep-old-tables' => 1 },
    $tables{foo1} . $tables{bar1}, $tables{foo2},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
## Options: keep-old-tables
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` ADD COLUMN `field` blob;
',
  ],

  'table-re' =>
  [
    { 'table-re' => 'ba' },
    $tables{foo1} . $tables{bar1} . $tables{baz1},
    $tables{foo2} . $tables{bar2} . $tables{baz2},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
## Options: table-re=ba
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `bar` ADD UNIQUE `name` (`name`,`age`);
ALTER TABLE `baz` ADD UNIQUE `firstname` (`firstname`,`surname`);
',
  ],

  'drop primary key with auto weirdness' =>
  [
    {},
    $tables{foo3},
    $tables{foo4},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` ADD INDEX (`id`); # auto columns must always be indexed
ALTER TABLE `foo` DROP PRIMARY KEY; # was (`id`)
ALTER TABLE `foo` ADD PRIMARY KEY (`id`,`foreign_id`);
ALTER TABLE `foo` DROP INDEX `id`;
',
  ],
      
  'drop additional primary key' =>
  [
    {},
    $tables{foo4},
    $tables{foo3},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` ADD INDEX (`id`); # auto columns must always be indexed
ALTER TABLE `foo` DROP PRIMARY KEY; # was (`id`,`foreign_id`)
ALTER TABLE `foo` ADD PRIMARY KEY (`id`);
ALTER TABLE `foo` DROP INDEX `id`;
',
  ],

  'unique changes' =>
  [
    {},
    $tables{bar1},
    $tables{bar2},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `bar` ADD UNIQUE `name` (`name`,`age`);
',
  ],
      
  'drop index' =>
  [
    {},
    $tables{bar2},
    $tables{bar1},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `bar` DROP INDEX `name`; # was UNIQUE (`name`,`age`)
',
  ],
      
  'alter indices' =>
  [
    {},
    $tables{bar2},
    $tables{bar3},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `bar` DROP INDEX `name`; # was UNIQUE (`name`,`age`)
ALTER TABLE `bar` ADD UNIQUE `id` (`id`,`name`,`age`);
',
  ],

  'alter indices 2' =>
  [
    {},
    $tables{bar3},
    $tables{bar2},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `bar` DROP INDEX `id`; # was UNIQUE (`id`,`name`,`age`)
ALTER TABLE `bar` ADD UNIQUE `name` (`name`,`age`);
',
  ],

  'add unique index' =>
  [
    {},
    $tables{bar1},
    $tables{bar3},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `bar` ADD UNIQUE `id` (`id`,`name`,`age`);
',
  ],

  'drop unique index' =>
  [
    {},
    $tables{bar3},
    $tables{bar1},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `bar` DROP INDEX `id`; # was UNIQUE (`id`,`name`,`age`)
',
  ],

  'alter unique index' =>
  [
    {},
    $tables{baz2},
    $tables{baz3},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `baz` DROP INDEX `firstname`; # was UNIQUE (`firstname`,`surname`)
ALTER TABLE `baz` ADD INDEX `firstname` (`firstname`,`surname`);
',
  ],

  'alter unique index 2' =>
  [
    {},
    $tables{baz3},
    $tables{baz2},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `baz` DROP INDEX `firstname`; # was INDEX (`firstname`,`surname`)
ALTER TABLE `baz` ADD UNIQUE `firstname` (`firstname`,`surname`);
',
  ],

  'alter unique index 3' =>
  [
    {},
    $tables{baz2},
    $tables{baz4},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `baz` DROP INDEX `firstname`; # was UNIQUE (`firstname`,`surname`)
ALTER TABLE `baz` ADD INDEX `users name` (`firstname`,`surname`);
',
  ],


  'add partition by hash' =>
  [
    {},
    $tables{qux1},
    $tables{qux2},
    '## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` PARTITION BY HASH (id);
',
  ],

  'add partition by hash' =>
  [
    {},
    $tables{qux1},
    $tables{qux2},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` PARTITION BY HASH (id);
},
  ],

  'add partition by linear hash' =>
  [
    {},
    $tables{qux1},
    $tables{qux3},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` PARTITION BY LINEAR HASH (id);
},
  ],

  'add partition by key' =>
  [
    {},
    $tables{qux1},
    $tables{qux4},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` PARTITION BY KEY (id);
},
  ],

  'add partition by linear key' =>
  [
    {},
    $tables{qux1},
    $tables{qux17},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` PARTITION BY LINEAR KEY (id);
},
  ],

  'add partition by range' =>
  [
    {},
    $tables{qux1},
    $tables{qux5},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` ADD INDEX (`id`); # auto columns must always be indexed
ALTER TABLE `foo` DROP PRIMARY KEY; # was (`id`)
ALTER TABLE `foo` ADD PRIMARY KEY (`id`,`create_at`);
ALTER TABLE `foo` DROP INDEX `id`;
ALTER TABLE `foo` PARTITION BY RANGE (TO_DAYS(create_at)) (PARTITION p20130314 VALUES LESS THAN (735306) ENGINE = InnoDB, PARTITION p20130328 VALUES LESS THAN (735320) ENGINE = InnoDB);
},
  ],

  'add partition by list' =>
  [
    {},
    $tables{qux1},
    $tables{qux6},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` ADD INDEX (`id`); # auto columns must always be indexed
ALTER TABLE `foo` DROP PRIMARY KEY; # was (`id`)
ALTER TABLE `foo` ADD PRIMARY KEY (`id`,`create_at`);
ALTER TABLE `foo` DROP INDEX `id`;
ALTER TABLE `foo` PARTITION BY LIST (MONTH(create_at)) (PARTITION odd VALUES IN (1,3,5,7,9,11) ENGINE = InnoDB, PARTITION even VALUES IN (2,4,6,8,10,12) ENGINE = InnoDB);
},
  ],

  'add list partition' =>
  [
    {},
    $tables{qux5},
    $tables{qux7},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` ADD PARTITION (PARTITION p20130329 VALUES LESS THAN (735321) ENGINE = InnoDB);
},
  ],

  'drop range partition' =>
  [
    {},
    $tables{qux7},
    $tables{qux5},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` DROP PARTITION p20130329;
},
  ],

  'add sub partition' =>
  [
    {},
    $tables{qux7},
    $tables{qux8},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` PARTITION BY RANGE (TO_DAYS(create_at)) SUBPARTITION BY HASH (id) SUBPARTITIONS 2 (PARTITION p20130314 VALUES LESS THAN (735306) ENGINE = InnoDB, PARTITION p20130328 VALUES LESS THAN (735320) ENGINE = InnoDB, PARTITION p20130329 VALUES LESS THAN (735321) ENGINE = InnoDB);
},
  ],

  'add sub partition' =>
  [
    {},
    $tables{qux7},
    $tables{qux18},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` PARTITION BY RANGE (TO_DAYS(create_at)) SUBPARTITION BY HASH (`id`) (PARTITION p20130314 VALUES LESS THAN (735306) (SUBPARTITION s0 ENGINE = InnoDB, SUBPARTITION s1 ENGINE = InnoDB), PARTITION p20130328 VALUES LESS THAN (735320) (SUBPARTITION s2 ENGINE = InnoDB, SUBPARTITION s3 ENGINE = InnoDB), PARTITION p20130329 VALUES LESS THAN (735321) (SUBPARTITION s4 ENGINE = InnoDB, SUBPARTITION s5 ENGINE = InnoDB));
},
  ],

  'add list partition with maxvalue' =>
  [
    {},
    $tables{qux9},
    $tables{qux10},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` REORGANIZE PARTITION pmax INTO (PARTITION p20130329 VALUES LESS THAN (735321) ENGINE = InnoDB, PARTITION pmax VALUES LESS THAN MAXVALUE ENGINE = InnoDB);
},
  ],

  'remove hash partition' =>
  [
    {},
    $tables{qux2},
    $tables{qux1},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` REMOVE PARTITIONING;
},
  ],

  'remove linear hash partition' =>
  [
    {},
    $tables{qux3},
    $tables{qux1},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` REMOVE PARTITIONING;
},
  ],

  'remove key partition' =>
  [
    {},
    $tables{qux4},
    $tables{qux1},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` REMOVE PARTITIONING;
},
  ],

  'remove linear key partition' =>
  [
    {},
    $tables{qux17},
    $tables{qux1},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` REMOVE PARTITIONING;
},
  ],

  'remove range partition' =>
  [
    {},
    $tables{qux5},
    $tables{qux1},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` REMOVE PARTITIONING;
ALTER TABLE `foo` ADD INDEX (`id`); # auto columns must always be indexed
ALTER TABLE `foo` DROP PRIMARY KEY; # was (`id`,`create_at`)
ALTER TABLE `foo` ADD PRIMARY KEY (`id`);
ALTER TABLE `foo` DROP INDEX `id`;
},
  ],

  'remove list partition' =>
  [
    {},
    $tables{qux6},
    $tables{qux1},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` REMOVE PARTITIONING;
ALTER TABLE `foo` ADD INDEX (`id`); # auto columns must always be indexed
ALTER TABLE `foo` DROP PRIMARY KEY; # was (`id`,`create_at`)
ALTER TABLE `foo` ADD PRIMARY KEY (`id`);
ALTER TABLE `foo` DROP INDEX `id`;
},
  ],


  'decrease range partitions' =>
  [
    {},
    $tables{qux11},
    $tables{qux12},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` COALESCE PARTITION 4;
},
  ],

  'increase range partitions' =>
  [
    {},
    $tables{qux12},
    $tables{qux11},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` ADD PARTITION PARTITIONS 4;
},
  ],


  'decrease linear hash partitions' =>
  [
    {},
    $tables{qux13},
    $tables{qux14},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` COALESCE PARTITION 4;
},
  ],

  'increase linear hash partitions' =>
  [
    {},
    $tables{qux14},
    $tables{qux13},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` ADD PARTITION PARTITIONS 4;
},
  ],

  'decrease key partitions' =>
  [
    {},
    $tables{qux15},
    $tables{qux16},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` COALESCE PARTITION 4;
},
  ],

  'increase key partitions' =>
  [
    {},
    $tables{qux16},
    $tables{qux15},
    qq{## mysqldiff <VERSION>
##
## Run on <DATE>
##
## --- file: tmp.db1
## +++ file: tmp.db2

ALTER TABLE `foo` ADD PARTITION PARTITIONS 4;
},
  ],


);

my %old_tests = %tests;
#%tests = (
  #'add partition' => $old_tests{'add partition by range'}
#);

my $BAIL = check_setup();
plan skip_all => $BAIL  if($BAIL);

use Data::Dumper;

my @tests = (keys %tests); #keys %tests

my $total = scalar(@tests) * 5;
plan tests => $total;

{
    my %debug = ( debug_file => 'debug.log', debug => 9 );
    unlink $debug{debug_file};

    for my $test (@tests) {
      note( "Testing $test\n" );

      my ($opts, $db1_defs, $db2_defs, $expected) = @{$tests{$test}};

      note("test=".Dumper($tests{$test}));

      my $diff = MySQL::Diff->new(%$opts, %debug);
      isa_ok($diff,'MySQL::Diff');

      my $db2 = get_db($db2_defs, 2);
      my $db1 = get_db($db1_defs, 1);

      my $d1 = $diff->register_db($db1, 1);
      my $d2 = $diff->register_db($db2, 2);
      note("d1=" . Dumper($d1));
      note("d2=" . Dumper($d2));

      isa_ok($d1, 'MySQL::Diff::Database');
      isa_ok($d2, 'MySQL::Diff::Database');

      my $diffs = $diff->diff();
      $diffs =~ s/^## mysqldiff [\d.]+/## mysqldiff <VERSION>/m;
      $diffs =~ s/^## Run on .*/## Run on <DATE>/m;
      $diffs =~ s{/\*!40\d{3} .*? \*/;\n*}{}m;
      $diffs =~ s/ *$//gm;
      for ($diffs, $expected) {
        s/ default\b/ DEFAULT/gi;
        s/PRIMARY KEY +\(/PRIMARY KEY (/g;
        s/auto_increment/AUTO_INCREMENT/gi;
      }

      my $engine = 'InnoDB';
      my $ENGINE_RE = qr/ENGINE=($VALID_ENGINES)/;
      if ($diffs =~ $ENGINE_RE) {
        $engine = $1;
        $expected =~ s/$ENGINE_RE/ENGINE=$engine/g;
      }

      note("diffs = "    . Dumper($diffs));
      note("expected = " . Dumper($expected));

      is_deeply($diffs, $expected, ".. expected differences for $test");

      # Now test that $diffs correctly patches $db1_defs to $db2_defs.
      my $patched = get_db($db1_defs . "\n" . $diffs, 1);
      $diff->register_db($patched, 1);
      is_deeply($diff->diff(), '', ".. patched differences for $test");
    }
}


sub get_db {
    my ($defs, $num) = @_;

    note("defs=$defs");

    my $file = "tmp.db$num";
    open(TMP, ">$file") or die "open: $!";
    print TMP $defs;
    close(TMP);
    my $db = MySQL::Diff::Database->new(file => $file, auth => { user => $TEST_USER });
    unlink $file;
    return $db;
}

sub check_setup {
    my $failure_string = "Cannot proceed with tests without ";
    _output_matches("mysql --help", qr/--password/) or
        return $failure_string . 'a MySQL client';
    _output_matches("mysqldump --help", qr/--password/) or
        return $failure_string . 'mysqldump';
    _output_matches("echo status | mysql -u $TEST_USER 2>&1", qr/Connection id:/) or
        return $failure_string . 'a valid connection';
    return '';
}

sub _output_matches {
    my ($cmd, $re) = @_;
    my ($exit, $out) = _run($cmd);

    my $issue;
    if (defined $exit) {
        if ($exit == 0) {
            $issue = "Output from '$cmd' didn't match /$re/:\n$out" if $out !~ $re;
        }
        else {
            $issue = "'$cmd' exited with status code $exit";
        }
    }
    else {
        $issue = "Failed to execute '$cmd'";
    }

    if ($issue) {
        warn $issue, "\n";
        return 0;
    }
    return 1;
}

sub _run {
    my ($cmd) = @_;
    unless (open(CMD, "$cmd|")) {
        return (undef, "Failed to execute '$cmd': $!\n");
    }
    my $out = join '', <CMD>;
    close(CMD);
    return ($?, $out);
}

package MySQL::Diff::Table;

=head1 NAME

MySQL::Diff::Table - Table Definition Class

=head1 SYNOPSIS

  use MySQL::Diff::Table

  my $db = MySQL::Diff::Database->new(%options);
  my $def           = $db->def();
  my $name          = $db->name();
  my $field         = $db->field();
  my $fields        = $db->fields();                # %$fields
  my $primary_key   = $db->primary_key();
  my $indices       = $db->indices();               # %$indices
  my $options       = $db->options();

  my $isfield       = $db->isa_field($field);
  my $isprimary     = $db->isa_primary($field);
  my $isindex       = $db->isa_index($field);
  my $isunique      = $db->is_unique($field);
  my $isfulltext    = $db->is_fulltext($field);

=head1 DESCRIPTION

Parses a table definition into component parts.

=cut

use warnings;
use strict;

our $VERSION = '0.43';

# ------------------------------------------------------------------------------
# Libraries

use Carp qw(:DEFAULT);
use MySQL::Diff::Utils qw(debug);

# ------------------------------------------------------------------------------

=head1 METHODS

=head2 Constructor

=over 4

=item new( %options )

Instantiate the objects, providing the command line options for database
access and process requirements.

=cut

sub new {
    my $class = shift;
    my %hash  = @_;
    my $self = {};
    bless $self, ref $class || $class;

    $self->{$_} = $hash{$_} for(keys %hash);

    debug(3,"\nconstructing new MySQL::Diff::Table");
    croak "MySQL::Diff::Table::new called without def params" unless $self->{def};
    $self->_parse;
    return $self;
}

=back

=head2 Public Methods

Fuller documentation will appear here in time :)

=over 4

=item * def

Returns the table definition as a string.

=item * name

Returns the name of the current table.

=item * field

Returns the current field definition of the given field.

=item * fields

Returns an array reference to a list of fields.

=item * primary_key

Returns a hash reference to fields used as primary key fields.

=item * indices

Returns a hash reference to fields used as index fields.

=item * options

Returns the additional options added to the table definition.

=item * isa_field

Returns 1 if given field is used in the current table definition, otherwise 
returns 0.

=item * isa_primary

Returns 1 if given field is defined as a primary key, otherwise returns 0.

=item * isa_index

Returns 1 if given field is used as an index field, otherwise returns 0.

=item * is_unique

Returns 1 if given field is used as unique index field, otherwise returns 0.

=item * is_fulltext

Returns 1 if given field is used as fulltext index field, otherwise returns 0.

=back

=cut

sub def             { my $self = shift; return $self->{def};            }
sub name            { my $self = shift; return $self->{name};           }
sub field           { my $self = shift; return $self->{fields}{$_[0]};  }
sub fields          { my $self = shift; return $self->{fields};         }
sub primary_key     { my $self = shift; return $self->{primary_key};    }
sub indices         { my $self = shift; return $self->{indices};        }
sub options         { my $self = shift; return $self->{options};        }
sub partitions      { my $self = shift; return $self->{partitions};     }

sub isa_field       { my $self = shift; return $_[0] && $self->{fields}{$_[0]}   ? 1 : 0; }
sub isa_primary     { my $self = shift; return $_[0] && $self->{primary}{$_[0]}  ? 1 : 0; }
sub isa_index       { my $self = shift; return $_[0] && $self->{indices}{$_[0]}  ? 1 : 0; }
sub is_unique       { my $self = shift; return $_[0] && $self->{unique}{$_[0]}   ? 1 : 0; }
sub is_fulltext     { my $self = shift; return $_[0] && $self->{fulltext}{$_[0]} ? 1 : 0; }

# ------------------------------------------------------------------------------
# Private Methods

sub _parse {
    my $self = shift;

    $self->{def} =~ s/`([^`]+)`/$1/gs;  # later versions quote names
    $self->{def} =~ s/\n+/\n/;
    $self->{lines} = [ grep ! /^\s*$/, split /(?=^)/m, $self->{def} ];
    my @lines = @{$self->{lines}};
    debug(4,"parsing table def '$self->{def}'");

    my $name;
    if ($lines[0] =~ /^\s*create\s+table\s+(\S+)\s+\(\s*$/i) {
        $self->{name} = $1;
        debug(3,"got table name '$self->{name}'");
        shift @lines;
    } else {
        croak "couldn't figure out table name";
    }

    my $partitions = '';
    my $is_partition = 0;
    while (@lines) {
        $_ = shift @lines;
        s!^\s+|\s+$!!g;
        s/^(.*?),?$/$1/ unless $is_partition; # trim whitespace and trailing commas
        debug(4,"line: [$_]");
        if (/^PRIMARY\s+KEY\s+(.+)$/) {
            my $primary = $1;
            croak "two primary keys in table '$self->{name}': '$primary', '$self->{primary_key}'\n"
                if $self->{primary_key};
            debug(4,"got primary key $primary");
            $self->{primary_key} = $primary;
            $primary =~ s/\((.*?)\)/$1/;
            $self->{primary}{$_} = 1    for(split(/,/, $primary));

            next;
        }

        if (/^(KEY|UNIQUE(?: KEY)?)\s+(\S+?)(?:\s+USING\s+(?:BTREE|HASH|RTREE))?\s*\((.*)\)$/) {
            my ($type, $key, $val) = ($1, $2, $3);
            croak "index '$key' duplicated in table '$name'\n"
                if $self->{indices}{$key};
            $self->{indices}{$key} = $val;
            $self->{unique}{$key} = 1   if($type =~ /unique/i);
            debug(4, "got ", defined $self->{unique}{$key} ? 'unique ' : '', "index key '$key': ($val)");
            next;
        }

        if (/^(FULLTEXT(?:\s+KEY|INDEX)?)\s+(\S+?)\s*\((.*)\)$/) {
            my ($type, $key, $val) = ($1, $2, $3);
            croak "FULLTEXT index '$key' duplicated in table '$name'\n"
                if $self->{fulltext}{$key};
            $self->{indices}{$key} = $val;
            $self->{fulltext}{$key} = 1;
            debug(4,"got FULLTEXT index '$key': ($val)");
            next;
        }

        if (/^\)\s*(.*?);?$/) {
            $self->{options} = $1;
            debug(4,"got table options '$self->{options}'");
            next;
        }

        if ($self->{options} && m#^/\*!50\d{3}\s+PARTITION\s+#) {
            $partitions = $_;
            $is_partition = 1;
            debug(4,"got table partitions '$_'");
            next;
        }elsif($is_partition){
            $partitions .= "\n$_";
            debug(4,"got table partitions '$_'");
            $is_partition = 0 if m!\*/;$!;
            next;
        }

        if (/^(\S+)\s*(.*)/) {
            my ($field, $fdef) = ($1, $2);
            croak "definition for field '$field' duplicated in table '$name'\n"
                if $self->{fields}{$field};
            $self->{fields}{$field} = $fdef;
            debug(4,"got field def '$field': $fdef");
            next;
        }

        croak "unparsable line in definition for table '$self->{name}':\n$_";
    }
    if($partitions){
      $self->{partitions} = MySQL::Diff::Table::Partition->new(def => $partitions);
    }

    warn "table '$self->{name}' didn't have terminator\n"
        unless defined $self->{options};

    @lines = grep ! m{^/\*!40\d{3} .*? \*/;}, @lines;
    @lines = grep ! m{^(SET |DROP TABLE)}, @lines;

    warn "table '$self->{name}' had trailing garbage:\n", join '', @lines
        if @lines;
}


package MySQL::Diff::Table::Partition;

use warnings;
use strict;

use Carp qw(:DEFAULT);
use MySQL::Diff::Utils qw(debug);

use overload
  'bool' => sub{ !!ref($_[0]) },
  '==' => \&_cmp,
  '!=' => \&_not_cmp,
;

sub def{
    my $def = shift->{def};

    $def =~ s!;$!!;
    $def =~ s{(?:^/\*!50\d+\s+)|(?:\s*\*/$)}{}g;
    return $def;
}

sub new {
    my $class = shift;
    my %hash  = @_;
    my $self = {};
    bless $self, ref $class || $class;

    $self->{$_} = $hash{$_} for(keys %hash);

    debug(3,"\nconstructing new MySQL::Diff::Table::Partition");
    croak "MySQL::Diff::Table::Partition::new called without def params" unless $self->{def};
    $self->_parse;
    return $self;
}

# ------------------------------------------------------------------------------
# Private Methods

sub _parse {
    my $self = shift;

    my $def = $self->def();

    $def =~ s!^\s*PARTITION\s*BY\s*((?:LINEAR\s+)?[^\s]+)\s*\((.*)\)[\r\n]*!!;
    $self->{type} = $1;
    $self->{field} = $2;

    if($def =~ s!^\s*SUBPARTITION\s*BY\s*([^\s]+)\s*\((.*)\)[\r\n\s]*SUBPARTITIONS\s+(\d+)[\r\n]*!!){
      $self->{subpartition} = {
        'type' => $1,
        'field' => $2,
        'partitions' => $3,
      };
    }

    $def =~ s{^[\r\n\s]*\(|\)[\r\n\s]*$}{}g;
    $self->{partitions} = [split /[\r\n]+/, $def];
}

sub _not_cmp{
    my ($self, $b) = @_;
    return $self->_cmp($b) ? 0 : 1;
}

sub _cmp{
    my ($self, $b) = @_;
    return 0 unless $b || ref($self) eq ref($b);

    foreach(qw/type field/){
        return 0 unless $self->{$_} eq $b->{$_};
    }

    if($self->{subpartition} || $b->{subpartition}){
        foreach(qw/type field partitions/){
            return 0 unless +(($self->{subpartition} || {})->{$_} || '') eq (($b->{subpartition} || {})->{$_} || '');
        }
    }

    if($self->{partitions} || $b->{partitions}){
        foreach my $p(@{$self->{partitions}}){
            return 0 unless grep{ $p eq $_ }@{$b->{partitions}};
        }
    }

    return 1;
}


1;

__END__

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2000-2011 Adam Spiers. All rights reserved. This
program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=head1 SEE ALSO

L<mysqldiff>, L<MySQL::Diff>, L<MySQL::Diff::Database>, L<MySQL::Diff::Utils>

=head1 AUTHOR

Adam Spiers <mysqldiff@adamspiers.org>

=cut

#! /usr/bin/env perl

use v5.16;
use strict;
use warnings;
use Carp;

use DBI;

# use defaults
use constant DNS => 'localhost';
use constant DATABASE => 'foo';
use constant USER => 'user';
use constant PASSWORD => 'passsword';
use constant PORT => 3306;

# Get the handle so we are allways ready to use
my $dbh = get_handle();
main();


sub get_handle {
  my $self = shift;

  my $dsn = "DBI:mysql:database=" . DATABASE . ";host=" . DNS . ";port=" . PORT;
  my $dbh = DBI->connect($dsn, USER, PASSWORD) or croak "Cannot establish connection " . $@;

  return $dbh;
}

sub create_table {
  my $self = shift;
  my $table_from_filename = shift;
  my $fields = shift;

  eval {
    $dbh->execute("desc ${table_from_filename}");
  };

  # if table already exists recreate another one with timestamp at the back
  $table_from_filename .= time if $@;

  say "Create new table ${table_from_filename}";

  # As we don't know about field
  # data types lets just make then
  # varchar(255 and deal with them later
  my $sql = "create table ${table_from_filename} ( " .
    join(', ', @{$fields} . ' varchar(255)') .
    ");";
  $dbh->execute($sql);

  return $table_from_filename;
}

sub insert_data {
  my $self = shift;
  my $table = shift;
  my $row = shift;
  my $fields = shift;

  my $sql = "insert into ${$table} ( " .
    join(",", @{$fields}) .
    ") values(" .
    join(",", @{$row}) . 
	")";

  eval {
    $dbh->execute($sql);
  };

  carp "Something happened with this data " . $@ if $@;
}

sub main {
  my $self = shift;
  my $filename = shift;

	croak "Must supply filename to import " unless $filename;
  # open with UTF-8 The reverant of our lives.
  open(my $fh, '<:encoding(UTF-8)', $filename) or croak "Cannot open file for reading " . $!;

  my @fields = ();
  my $table_created = 0;
  my $table_name = $filename;

  while (my $row = <$fh>) {
    $row =~ s/\r\n?|\n//g;

    my @data = split(',', $row);

    if ($table_created) {

      $self->insert_data($table_name, \@data, \@fields);

    } else {
      @fields = @data;
      $table_name = $self->create_table($filename, \@fields);
      $table_created = 1;
    }
  }

}

# NAME

Bio::SeqWare::Uploads::CgHub::Fastq - Support uploads of fastq files to cghub

# VERSION

Version 0.000.001   \# PRE-RELEASE

# SYNOPSIS





Perhaps a little code snippet.

    use Bio::SeqWare::Uploads::CgHub::Fastq;

    my $obj = Bio::SeqWare::Uploads::CgHub::Fastq->new();

# DESCRIPTION

Supports the upload of zipped fastq file sets for samples to cghub. Includes
db interactions, zip command line convienience functions, and meta-data
generation control. The meta-data uploads are a hack on top of a current
implementation, just generates the current version, then after-the-fact
modifies it to do a fastq upload.

# CLASS METHODS

## new()

    my $obj = Bio::SeqWare::Uploads::CgHub::Fastq->new();

Creates and returns a Bio::SeqWare::Uploads::CgHub::Fastq object. Takes
no parameters.

# INSTANCE METHODS

    NONE

# INTERNAL METHODS

NOTE: These methods are for _internal use only_. They are documented here
mainly due to the effort needed to separate user and developer documentation.
Pay no attention to code behind the curtain; these are not the methods you are
looking for. If you use these function _you are doing something wrong._

    NONE

# AUTHOR

Stuart R. Jefferys, `<srjefferys (at) gmail (dot) com>`

# DEVELOPMENT

This module is developed and hosted on GitHub, at
["/github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq" in p5-Bio-SeqWare-Config https:](http://search.cpan.org/perldoc?p5-Bio-SeqWare-Config https:#/github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq).
It is not currently on CPAN, and I don't have any immediate plans to post it
there unless requested by core SeqWare developers (It is not my place to
set out a module name hierarchy for the project as a whole :)

# INSTALLATION

You can install a version of this module directly from github using

    $ cpanm git://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq.git@v0.000.001

Any version can be specified by modifying the tag name, following the @;
the above installs the latest _released_ version. If you leave off the @version
part of the link, you can install the bleading edge pre-release, if you don't
care about bugs...

You can select and download any package for any released version of this module
directly from [https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq/releases](https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq/releases).
Installing is then a matter of unzipping it, changing into the unzipped
directory, and then executing the normal (C>Module::Build>) incantation:

     perl Build.PL
     ./Build
     ./Build test
     ./Build install

# BUGS AND SUPPORT

No known bugs are present in this release. Unknown bugs are a virtual
certainty. Please report bugs (and feature requests) though the
Github issue tracker associated with the development repository, at:

[https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq/issues](https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq/issues)

Note: you must have a GitHub account to submit issues.

# ACKNOWLEDGEMENTS

This module was developed for use with [SegWare ](http://search.cpan.org/perldoc?http:#/seqware.github.io).

# LICENSE AND COPYRIGHT

Copyright 2013 Stuart R. Jefferys.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

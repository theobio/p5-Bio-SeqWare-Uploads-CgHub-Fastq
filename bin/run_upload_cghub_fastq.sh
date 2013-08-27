#!/bin/bash

# USAGE:
#    Run the upload-cghub-fastq perl script with optional stage
#    $ run_upload-cghub-fastq.sh [ZIP | META | VALIDATE | SUBMIT | ALL ]

VERSION="0.000.004"

STAGE="ZIP"
if [[ -z "$1" ]]; then
    STAGE=$1
fi

# Enable perlbrew and switch to uncseqperl until done
export PERLBREW_ROOT="/home/seqware/perl5/perlbrew"
export PERLBREW_HOME="/tmp/.perlbrew"
source "${PERLBREW_ROOT}/etc/bashrc"
perlbrew use uploadperl

COMMAND="upload-cghub-fastq.pl --verbose --runMode $STAGE"

HOST=`hostname`
DATE=`date`
LOG_HEADING_MESSAGE=" $HOST $DATE"

echo ""
echo "-----------------------------------------------------------------"
echo $LOG_HEADING_MESSAGE
echo "-----------------------------------------------------------------"
echo "PROGRAM: run_upload-cghub-fastq.sh $VERSION."
echo "RUNNING: $COMMAND"

$COMMAND

# Not really needed as redirections go away when close top shell, but tidy.
perlbrew off

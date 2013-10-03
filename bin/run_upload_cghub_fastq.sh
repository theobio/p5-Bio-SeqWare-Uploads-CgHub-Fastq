#!/bin/bash

# USAGE:
# Run the upload-cghub-fastq perl script with optional stage
#    $ run_upload-cghub-fastq.sh [STAGE]
# Where
#    STAGE = (optionally) one of ZIP (deault) | META | VALIDATE | SUBMIT | ALL
#
# Assumes that $PERLBREW_ALIAS holds the name of the perlbrew install to use.
# If not defined, just uses the current perl

#
# Set up defaults and initialize log.
#

VERSION="0.000.024"
STAGE="$1"

if [ -z "$STAGE" ]; then
    STAGE="ZIP"
fi

HOST=`hostname`
DATE=`date`
LOG_HEADING_MESSAGE=" $HOST $DATE"

echo ""
echo "-----------------------------------------------------------------"
echo $LOG_HEADING_MESSAGE
echo "-----------------------------------------------------------------"
echo "PROGRAM: $0 $VERSION."

#
# Want to exit if the average load is too high. A reasonable maximum average
# load is the number of CPUs/2 (rounded down), + 1
#

LOAD=$(cut -d " " -f1 /proc/loadavg)
LOAD=$(printf "%.0f" $LOAD)          # Bash math, no decimals allowed
NCPU=$(grep -c 'processor' /proc/cpuinfo)
MAX_LOAD=$((($NCPU/2) +1))
if [ $LOAD -gt $MAX_LOAD ]; then
   echo "NOT RUNNING $0: Too busy. Load $LOAD exceeds max load $MAX_LOAD."
   exit 1;
fi;

#
# LOAD is ok. Go for execution
#

echo "Current average load: $LOAD; Max load: $MAX_LOAD."

# Enable perlbrew and switch to uncseqperl until done
export PERLBREW_ROOT="/home/seqware/perl5/perlbrew"
export PERLBREW_HOME="/tmp/.perlbrew"
source "${PERLBREW_ROOT}/etc/bashrc"
if [ -z "${PERLBREW_ALIAS}" ]; then
    perlbrew use
else
    perlbrew use ${PERLBREW_ALIAS}
    perlbrew use
fi

#
# Set up and run the perl script that does (at least some stage of) the
# fastq upload process
#

COMMAND="upload-cghub-fastq.pl --verbose --runMode $STAGE"
echo "RUNNING: $COMMAND"
$COMMAND

# Not really needed as redirections go away when close top shell, but tidy.
perlbrew off

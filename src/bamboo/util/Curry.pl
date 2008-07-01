#!/usr/bin/perl -w
#
# Copyright (c) 2001-2003 Regents of the University of California.
# All rights reserved.
#
# See the file LICENSE included in this distribution for details.
#
# $Id: Curry.pl,v 1.2 2005/08/17 17:34:02 srhea Exp $
#

use strict;

my $MAX_ARGS = 10;

print <<"EOF";
/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.util;

/**
 * Parameterized function object types and currying functions, modeled on
 * libasync's <code>wrap</code> function.  
 * 
 * <p> There are two kinds of function objects defined in this file.  "Thunks"
 * are those that return <code>void</code>, and "Functions" are those that
 * return a value.  This distinction is due to the fact that you can't declare
 * a type of <code>void</code> in a parameterized Java object.  Also, the name
 * of each kind of function object includes its argument count, since you
 * can't have an unbounded number of parameters to a type.  So
 * <code>Thunk1&lt;Integer&gt;</code> takes an <code>Integer</code> argument
 * and has return type <code>void</code>, whereas
 * <code>Function2&lt;Double,Integer,Boolean&gt;</code> takes two arguments,
 * one <code>Integer</code> and one <code>Boolean</code> and returns a
 * <code>Double</code>.
 *
 * <p> There is no Thunk0; instead, we use java.lang.Runnable so that we can
 * easily use these function objects with other Java libraries--such as
 * Swing--that use Runnable for function objects.
 *
 * <i>Note: this class is automatically generated
 * by <code>Curry.pl</code>; do not edit the <code>.java</code> file.</i>
 */
public class Curry {
EOF

# prints <A_start, ... , A_finish>
sub print_types {
    my ($start, $finish) = @_;
    if ($finish-$start >= 0) {
        print "<";
        my $j = $start-1;
        while ($j++ < $finish) { 
            print "A$j"; if ($j < $finish) { print ", "; }
        }
        print ">";
    }
}

# prints <R, A_start, ... , A_finish>
sub print_types_result {
    my ($start, $finish) = @_;
    print "<R";
    if ($finish-$start >= 0) {
        my $j = $start-1;
        while ($j++ < $finish) { 
            print ", A$j";
        }
    }
    print ">";
}

# prints A_start a_start, ... , A_finish a_finish
sub print_args {
    my ($start, $finish) = @_;
    if ($finish > 0) {
        my $j = $start-1;
        while ($j++ < $finish) { 
            print "A$j a$j"; if ($j < $finish) { print ", "; }
        }
    }
}

# prints A_start a_start, ... , A_finish a_finish
sub print_in_args {
    my ($start, $finish, $indent) = @_;
    if ($finish > 0) {
        my $j = $start-1;
        while ($j++ < $finish) { 
            print "final A$j a$j"; 
            if ($j < $finish) { print ",\n$indent"; }
        }
    }
}

# prints a_start, ... , a_finish
sub print_formals {
    my ($start, $finish) = @_;
    if ($finish > 0) {
        my $j = $start - 1;
        while ($j++ < $finish) { 
            print "a$j"; if ($j < $finish) { print ", "; }
        }
    }
}

for (my $i = 1; $i <= $MAX_ARGS; ++$i) {

    print "\n";
    print "    public interface Thunk$i";
    &print_types (1,$i);
    if ($i == 0) {
        print " extends Runnable";
    }
    print " {\n";

    print "        void run(";
    &print_args (1, $i);
    print ");\n";
    print "    }\n";
}

for (my $i = 0; $i <= $MAX_ARGS; ++$i) {
    print "\n";
    print "    public interface Function$i";
    &print_types_result (1,$i);
    print " {\n";

    print "        R run(";
    &print_args (1, $i);
    print ");\n";
    print "    }\n";
}

for (my $i = 0; $i <= $MAX_ARGS; ++$i) {
    for (my $j = 0; $j < $i; ++$j) {
        print "\n";
        print "    public static ";
        &print_types (1,$i);
        if ($j == 0) {
            print " Runnable";
        } else {
            print " Thunk$j";
        }
        &print_types ($i-$j+1,$i);
        print " curry (\n            final Thunk$i";
        &print_types (1,$i);
        print " f,\n            ";
        &print_in_args (1, $i-$j, "            ");
        print ") {\n";
        if ($j == 0) {
            print "        return new Runnable";
        } else {
            print "        return new Thunk$j";
        }
        &print_types ($i-$j+1, $i);
        print " () {\n";
#        print "            private Thunk$i";
#        &print_types (1, $i);
#        print " f = in_f;\n";
#        for (my $k = 1; $k <= $i-$j; ++$k) {
#            print "            private A$k a$k = in_a$k;\n";
#        }
        print "            public void run(";
        &print_args ($i-$j+1, $i);
        print ") {\n";
        print "                f.run(a1";
        for (my $k = 2; $k <= $i; ++$k) {
            print ", a$k";
        }
        print ");\n";
        print "            }\n";
        print "        };\n";
        print "    }\n";
    }
}

for (my $i = 0; $i <= $MAX_ARGS; ++$i) {
    for (my $j = 0; $j < $i; ++$j) {
        print "\n";
        print "    public static ";
        &print_types_result (1,$i);
        print " Function$j";
        &print_types_result ($i-$j+1,$i);
        print " curry (\n            final Function$i";
        &print_types_result (1,$i);
        print " f,\n            ";
        &print_in_args (1, $i-$j, "            ");
        print ") {\n";
        print "        return new Function$j";
        &print_types_result ($i-$j+1, $i);
        print " () {\n";
#        print "            private Function$i";
#        &print_types_result (1, $i);
#        print " f = in_f;\n";
#        for (my $k = 1; $k <= $i-$j; ++$k) {
#            print "            private A$k a$k = in_a$k;\n";
#        }
        print "            public R run(";
        &print_args ($i-$j+1, $i);
        print ") {\n";
        print "                return f.run(";
        &print_formals (1, $i);
        print ");\n";
        print "            }\n";
        print "        };\n";
        print "    }\n";
    }
}

print "}\n";


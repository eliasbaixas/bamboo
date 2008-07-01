# $Id: test-5-virtual.gnuplot,v 1.2 2004/10/06 20:35:21 srhea Exp $
set title "Same Size, Various TTL Puts"
set xlabel "Time (min)"
set ylabel "Acheived storage (MBs)"
set xtics 0,60
set mxtics 4
set mytics 4
set key below
set terminal postscript eps color solid "Times-Roman" 30
set output 'test-5-virtual.eps'
plot 'test-5-virtual.dat' using ($1/1000/60):($2/1024/1024) \
     ti "2" with li, \
     'test-5-virtual.dat' using ($1/1000/60):($3/1024/1024) \
     ti "4" with li, \
     'test-5-virtual.dat' using ($1/1000/60):($4/1024/1024) \
     ti "8" with li, \
     'test-5-virtual.dat' using ($1/1000/60):($5/1024/1024) \
     ti "16" with li, \
     'test-5-virtual.dat' using ($1/1000/60):($6/1024/1024) \
     ti "32" with li, \
     'test-5-virtual.dat' using ($1/1000/60):($7/1024/1024) \
     ti "60" with li lt 7
#pause -1

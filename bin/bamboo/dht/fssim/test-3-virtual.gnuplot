# $Id: test-3-virtual.gnuplot,v 1.1 2004/09/24 17:19:49 srhea Exp $
set xlabel "Time (min)"
set ylabel "Acheived rate (puts/s)"
set xtics 0,60
set mxtics 4
set mytics 4
set key 240,0.27
set terminal postscript eps color solid "Times-Roman" 30
set output 'test-3-virtual.eps'
plot 'test-3-virtual.dat' using ($1/1000/60):($2/60/60/1024) \
    ti "10/s" with li, \
     'test-3-virtual.dat' using ($1/1000/60):($5/60/60/1024) \
     ti "1/s" with li, \
     'test-3-virtual.dat' using ($1/1000/60):($20/60/60/1024) \
     ti "1/10s" with li, \
     'test-3-virtual.dat' using ($1/1000/60):($70/60/60/1024) \
     noti with li
#pause -1

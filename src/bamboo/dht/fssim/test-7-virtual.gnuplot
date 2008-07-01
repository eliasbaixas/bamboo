# $Id: test-7-virtual.gnuplot,v 1.2 2004/10/06 20:35:21 srhea Exp $
set title "Bursty Client Test"
set xlabel "Time (min)"
set ylabel "Acheived storage (MBs)"
set xtics 0,60
set mxtics 4
set mytics 4
set key below
set terminal postscript eps color solid "Times-Roman" 30
set output 'test-7-virtual.eps'
plot 'test-7-virtual.dat' using ($1/1000/60):($3/1024/1024) \
     ti "Fast" with li, \
     'test-7-virtual.dat' using ($1/1000/60):($2/1024/1024) \
     ti "Faster" with li, \
     'test-7-virtual.dat' using ($1/1000/60):($4/1024/1024) \
     ti "Bursty" with li
#     'test-7-virtual.dat' using ($1/1000/60):($5/1024/1024) \
#     ti "Bursty 2" with li, \
#     'test-7-virtual.dat' using ($1/1000/60):($6/1024/1024) \
#     ti "Bursty 3" with li, \
#     'test-7-virtual.dat' using ($1/1000/60):($7/1024/1024) \
#     ti "Bursty 4" with li

# $Id: test-4-rate.gnuplot,v 1.1 2004/09/15 22:11:38 srhea Exp $
set xlabel "Time (min)"
set ylabel "Storage (MB)"
set xtics 0,60
set mxtics 4
set mytics 4
set key bottom right
set terminal postscript eps color solid "Times-Roman" 20
set output 'test-4-rate.eps'
plot \
     'test-4-rate.dat' using ($1/1000/60):($9/1024/1024) \
     ti "1024" with li, \
     'test-4-rate.dat' using ($1/1000/60):($8/1024/1024) \
     ti "896" with li, \
     'test-4-rate.dat' using ($1/1000/60):($7/1024/1024) \
     ti "768" with li, \
     'test-4-rate.dat' using ($1/1000/60):($6/1024/1024) \
     ti "640" with li, \
     'test-4-rate.dat' using ($1/1000/60):($5/1024/1024) \
     ti "512" with li, \
     'test-4-rate.dat' using ($1/1000/60):($4/1024/1024) \
     ti "384" with li, \
     'test-4-rate.dat' using ($1/1000/60):($3/1024/1024) \
     ti "256" with li, \
     'test-4-rate.dat' using ($1/1000/60):($2/1024/1024) \
     ti "128" with li
#pause -1

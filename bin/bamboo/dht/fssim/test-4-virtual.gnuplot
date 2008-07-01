# $Id: test-4-virtual.gnuplot,v 1.3 2004/10/24 02:27:18 srhea Exp $
#set title "Same TTL, Various Size Puts"
set xlabel "Time (hours)"
set ylabel "Cumulative Storage-Over-Time\n(MB*hours)"
#set xtics 0,60
#set mxtics 4
set xrange [0:2]
set mytics 4
set key below
set terminal postscript eps color solid "Times-Roman" 20
set output 'test-4-virtual.eps'
plot 'test-4-bytessecs.dat' using ($1/1000/60/60):($2/1024/1024) \
     ti "Fastest" with li, \
     'test-4-bytessecs.dat' using ($1/1000/60/60):($3/1024/1024) \
     ti "Fast" with li, \
     'test-4-bytessecs.dat' using ($1/1000/60/60):($4/1024/1024) \
     ti "1 kB x 1 hr" with li, \
     'test-4-bytessecs.dat' using ($1/1000/60/60):($5/1024/1024) \
     ti "1 kB x 15 min" with li, \
     'test-4-bytessecs.dat' using ($1/1000/60/60):($6/1024/1024) \
     ti "1 kB x 5 min" with li, \
     'test-4-bytessecs.dat' using ($1/1000/60/60):($7/1024/1024) \
     ti "256 B x 1 hr" with li lt 7, \
     'test-4-bytessecs.dat' using ($1/1000/60/60):($8/1024/1024) \
     ti "32 B x 1 hr" with li lt 8
#pause -1

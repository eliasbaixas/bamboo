# $Id: test-2-rate.gnuplot,v 1.2 2004/09/24 17:19:49 srhea Exp $
set xlabel "Time (hours)"
set ylabel "Total Storage (MB)"
set xtics 0,1
set mxtics 2
set key 5.5,2.7
set terminal postscript eps color solid "Times-Roman" 30
set output 'test-2-rate.eps'
plot 'test-2-rate.dat' us ($1/1000/3600):($2/1024/1024) ti "1" with li lw 3, \
     'test-2-rate.dat' us ($1/1000/3600):($3/1024/1024) ti "2" with li lw 3, \
     'test-2-rate.dat' us ($1/1000/3600):($4/1024/1024) ti "3" with li lw 3, \
     'test-2-rate.dat' us ($1/1000/3600):($5/1024/1024) ti "4" with li lw 3, \
     'test-2-rate.dat' us ($1/1000/3600):($6/1024/1024) ti "5" with li lw 3
#pause -1

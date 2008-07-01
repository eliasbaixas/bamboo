# $Id: test-4-storage.gnuplot,v 1.1 2004/09/15 22:11:38 srhea Exp $
set xlabel "Time (min)"
set ylabel "Acheived rate (puts/s)"
set xtics 0,60
set mxtics 4
set mytics 4
set key 240,0.27
set terminal postscript eps color solid "Times-Roman" 30
set output 'test-4-storage.eps'
plot 'test-4-storage.dat' using ($1/1000/60):($2/60/60/1024) \
    ti "10/s 1024 byte" with li, \
     'test-4-storage.dat' using ($1/1000/60):($3/60/60/1024) \
     ti "1/s 1024 byte" with li, \
     'test-4-storage.dat' using ($1/1000/60):($4/60/60/1024) \
     ti "1/10s 1024 byte" with li, \
     'test-4-storage.dat' using ($1/1000/60):($5/60/60/1024) \
     ti "1/10s 1024 byte" with li, \
     'test-4-storage.dat' using ($1/1000/60):($6/60/60/1024) \
     ti "1/10s 1024 byte" with li, \
     'test-4-storage.dat' using ($1/1000/60):($7/60/60/1024) \
     ti "1/10s 1024 byte" with li, \
     'test-4-storage.dat' using ($1/1000/60):($8/60/60/1024) \
     ti "1/10s 1024 byte" with li, \
     'test-4-storage.dat' using ($1/1000/60):($9/60/60/1024) \
     ti "1/5s 512 byte" with li
#pause -1
